#!/usr/bin/env python3
"""M2 测试：RunRegistry 状态机 + 多 turn 串行 + steer/queue + 失败路径 + 隐式 run + GC。

运行：python3 tests/test_m2.py
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


async def setup():
    gw = MockGateway(port=GW_PORT)
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
    gc = GatewayClient(f"ws://127.0.0.1:{GW_PORT}/api/ws")
    await gc.connect()
    bs = BridgeServer(gc)
    server = await asyncio.start_server(bs.handle, "127.0.0.1", BRIDGE_PORT)
    return gw, gw_task, gc, bs, server


async def full_output(client: StudioClient, run_id: str) -> str:
    r = await client.request({"action": "get_output", "run_id": run_id,
                              "cursor": 0, "event_cursor": 0})
    return r.get("output", "")


async def main():
    gw, gw_task, gc, bs, server = await setup()
    client = StudioClient()
    await asyncio.sleep(0.2)

    print("== M2 测试（RunRegistry / 多 turn / GC）==\n")

    # ── 1. 顺序多 turn：等前一个完成再发下一个，两个 run 独立、输出正确 ──
    r1 = await client.request({"action": "chat", "session_id": "m2-seq", "message": "first"})
    f1 = await client.wait_done(r1["run_id"])
    check("顺序.run1完成", f1.get("done") is True and f1.get("status") == "completed", f1.get("status"))
    r2 = await client.request({"action": "chat", "session_id": "m2-seq", "message": "second"})
    f2 = await client.wait_done(r2["run_id"])
    check("顺序.run_id不同", r1["run_id"] != r2["run_id"], (r1.get("run_id"), r2.get("run_id")))
    check("顺序.run2完成", f2.get("done") is True and f2.get("status") == "completed", f2.get("status"))
    o1 = await full_output(client, r1["run_id"])
    o2 = await full_output(client, r2["run_id"])
    check("顺序.run1输出", "Mock reply to: first" in o1, o1)
    check("顺序.run2输出", "Mock reply to: second" in o2, o2)

    # ── 2. steer：第二个 chat 在流进行中提交 → 旧 run 结束、新 run 收后续 ──
    r1 = await client.request({"action": "chat", "session_id": "m2-steer", "message": "base"})
    await asyncio.sleep(0.12)  # 让第一块 delta 先到
    r2 = await client.request({"action": "chat", "session_id": "m2-steer", "message": "extra"})
    check("steer.第二个run创建", r2.get("ok") is True and r2.get("run_id"), r2)
    f1 = await client.wait_done(r1["run_id"])
    f2 = await client.wait_done(r2["run_id"])
    check("steer.旧run不悬挂(完成)", f1.get("done") is True, f1.get("status"))
    check("steer.新run完成", f2.get("done") is True, f2.get("status"))
    o1 = await full_output(client, r1["run_id"])
    o2 = await full_output(client, r2["run_id"])
    combined = o1 + o2
    check("steer.基础回复完整", "Mock reply to: base" in combined, combined)
    check("steer.转向文本并入", "[steered: extra]" in combined, combined)

    # ── 3. queued：busy_mode=queue → 两个 turn 顺序执行、输出各自正确 ──
    gw.busy_mode = "queue"
    r1 = await client.request({"action": "chat", "session_id": "m2-queue", "message": "q1"})
    await asyncio.sleep(0.12)
    r2 = await client.request({"action": "chat", "session_id": "m2-queue", "message": "q2"})
    f1 = await client.wait_done(r1["run_id"])
    f2 = await client.wait_done(r2["run_id"])
    check("queue.run1完成", f1.get("done") is True, f1.get("status"))
    check("queue.run2完成", f2.get("done") is True, f2.get("status"))
    o1 = await full_output(client, r1["run_id"])
    o2 = await full_output(client, r2["run_id"])
    check("queue.run1输出=第一turn", "Mock reply to: q1" in o1, o1)
    check("queue.run2输出=第二turn", "Mock reply to: q2" in o2, o2)
    gw.busy_mode = "steer"

    # ── 4. 失败路径：message.complete 带 error → run failed ──
    gw.fail_mode = True
    r = await client.request({"action": "chat", "session_id": "m2-fail", "message": "boom"})
    f = await client.wait_done(r["run_id"])
    check("失败.status=failed", f.get("status") == "failed", f.get("status"))
    check("失败.done=true", f.get("done") is True, f.get("done"))
    check("失败.error透传", "mock agent failure" in str(f.get("error", "")), f.get("error"))
    gw.fail_mode = False

    # ── 5. 隐式 run：无 chat 先行的事件（后台 turn）→ 自动建 run ──
    await gc.call("test.emit", {"session_id": "tui-bg-1"})
    await asyncio.sleep(0.3)
    implicit = [r for r in gc.runs.values() if r.tui_session_id == "tui-bg-1"]
    check("隐式.run创建", len(implicit) == 1, len(implicit))
    if implicit:
        check("隐式.status=completed", implicit[0].status == "completed", implicit[0].status)
        check("隐式.output", "bg text" in implicit[0].result_view()["output"],
              implicit[0].result_view()["output"])

    # ── 6. run GC：超期已完成 run 被清理 ──
    gc.run_retain_s = 0.3
    r = await client.request({"action": "chat", "session_id": "m2-gc", "message": "gc me"})
    f = await client.wait_done(r["run_id"])
    check("GC.run完成", f.get("done") is True, f.get("status"))
    await asyncio.sleep(0.5)
    gc.gc_runs()
    r2 = await client.request({"action": "get_output", "run_id": r["run_id"],
                              "cursor": 0, "event_cursor": 0})
    check("GC.run被清理", r2.get("ok") is False and r2.get("error_type") == "KeyError", r2)
    gc.run_retain_s = 1800.0

    # ── 清理 ──
    server.close()
    gw_task.cancel()
    await gc.close()

    print(f"\n结果: {passed} 通过, {failed} 失败")
    if failures:
        print("\n失败明细:")
        for f in failures:
            print(f"  - {f}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())