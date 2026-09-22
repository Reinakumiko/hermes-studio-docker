#!/usr/bin/env python3
"""M6 测试：WS 断线重连 / session.events.since 回放 / 心跳。

运行：python3 tests/test_m6.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mock_gateway import MockGateway              # noqa: E402
from bridge import GatewayClient, BridgeServer   # noqa: E402

BRIDGE_PORT = 18765
GW_PORT = 19119

passed = 0
failed = 0
failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        failures.append(f"{name}: {detail}")
        print(f"  ❌ {name}  {detail}")


class StudioClient:
    async def request(self, req: dict, timeout: float = 10.0) -> dict:
        reader, writer = await asyncio.open_connection("127.0.0.1", BRIDGE_PORT)
        try:
            writer.write((json.dumps(req) + "\n").encode())
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            return json.loads(line)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def wait_done(self, run_id: str, max_s: float = 10.0) -> dict:
        cursor = event_cursor = 0
        last = {}
        for _ in range(int(max_s / 0.1)):
            last = await self.request({"action": "get_output", "run_id": run_id,
                                       "cursor": cursor, "event_cursor": event_cursor})
            cursor = last.get("cursor", cursor)
            event_cursor = last.get("event_cursor", event_cursor)
            if last.get("done"):
                return last
            await asyncio.sleep(0.1)
        return last


async def start_mock(**kw):
    gw = MockGateway(port=GW_PORT)
    for k, v in kw.items():
        setattr(gw, k, v)
    gw_task = asyncio.create_task(gw.run())
    import websockets
    for _ in range(30):
        try:
            async with websockets.connect(f"ws://127.0.0.1:{GW_PORT}/api/ws"):
                break
        except Exception:
            await asyncio.sleep(0.2)
    else:
        raise RuntimeError("mock gateway 启动失败")
    return gw, gw_task


async def main():
    gw, gw_task = await start_mock()
    gc = GatewayClient(f"ws://127.0.0.1:{GW_PORT}/api/ws")
    gc.reconnect_min_delay = 0.3   # 测试加速
    await gc.connect()
    bs = BridgeServer(gc)
    server = await asyncio.start_server(bs.handle, "127.0.0.1", BRIDGE_PORT)
    client = StudioClient()
    await asyncio.sleep(0.2)

    print("== M6 测试（重连 / 回放 / 心跳）==\n")

    # ── 1. 断线重连：kick 后自动重连，chat 恢复 ──
    r = await client.request({"action": "chat", "session_id": "m6-a", "message": "before"})
    await client.wait_done(r["run_id"])
    conns_before = gw.connection_count
    await gc.call("test.kick", {})
    # 等重连完成
    for _ in range(40):
        if gw.connection_count > conns_before:
            break
        await asyncio.sleep(0.2)
    check("重连.建立新连接", gw.connection_count > conns_before,
          (conns_before, gw.connection_count))
    await asyncio.sleep(0.5)  # 等 capabilities + 回放完成
    r = await client.request({"action": "chat", "session_id": "m6-a", "message": "after"})
    f = await client.wait_done(r["run_id"])
    check("重连.chat恢复", f.get("done") is True and "Mock reply to: after" in f.get("output", ""),
          f.get("status"))

    # ── 2. 回放：seq 水位 + session.events.since 调用 + 事件再处理 ──
    tui_sid = gc.sessions["m6-a"]
    watermark = gc.session_event_seq.get(tui_sid, 0)
    check("回放.seq水位已跟踪", watermark > 0, gc.session_event_seq)
    # 注入一条"断线期间发生"的事件到 mock 回放日志（seq = 水位+1）
    missed = {"type": "message.complete", "session_id": tui_sid,
              "text": "bg while away", "seq": gw._seq.get(tui_sid, 0) + 1}
    gw._seq[tui_sid] = missed["seq"]
    gw.event_log.setdefault(tui_sid, []).append(dict(missed))
    # kick → 重连 → 回放
    await gc.call("test.kick", {})
    for _ in range(40):
        if any(c[0] == tui_sid and c[1] == watermark for c in gw.since_calls):
            break
        await asyncio.sleep(0.2)
    check("回放.since被调用",
          any(c[0] == tui_sid and c[1] == watermark for c in gw.since_calls), gw.since_calls)
    await asyncio.sleep(0.3)
    # 回放的事件被处理：隐式 run 完成且 result 为错过的事件文本
    implicit = [r for r in gc.runs.values()
                if r.tui_session_id == tui_sid and r.result == "bg while away"]
    check("回放.错过事件被处理", len(implicit) == 1,
          [(r.run_id, r.status, r.result) for r in gc.runs.values()])

    # ── 3. 心跳：ready.heartbeat=true → 周期 ping ──
    server.close()
    await gc.close()
    gw_task.cancel()
    await asyncio.sleep(0.5)  # 等端口释放
    gw2, gw2_task = await start_mock(ready_heartbeat=True)
    gc2 = GatewayClient(f"ws://127.0.0.1:{GW_PORT}/api/ws")
    gc2.heartbeat_interval = 0.5
    await gc2.connect()
    await asyncio.sleep(1.6)
    check("心跳.周期ping", gw2.ping_count >= 2, gw2.ping_count)
    await gc2.close()
    gw2_task.cancel()

    # ── 清理 ──
    server.close()
    gw_task.cancel()

    print(f"\n结果: {passed} 通过, {failed} 失败")
    if failures:
        print("\n失败明细:")
        for f in failures:
            print(f"  - {f}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())