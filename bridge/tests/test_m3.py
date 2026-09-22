#!/usr/bin/env python3
"""M3 测试：交互闭环——clarify 全链路 / sudo -32601 / request.cancel 清理。

运行：python3 tests/test_m3.py
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

    async def wait_event(self, run_id: str, event_name: str, max_s: float = 10.0) -> dict | None:
        event_cursor = 0
        for _ in range(int(max_s / 0.1)):
            r = await self.request({"action": "get_output", "run_id": run_id,
                                    "cursor": 0, "event_cursor": event_cursor})
            event_cursor = r.get("event_cursor", event_cursor)
            for e in r.get("events", []):
                if e.get("event") == event_name:
                    return e
            if r.get("done"):
                # done 后再全量扫一次
                r_all = await self.request({"action": "get_output", "run_id": run_id,
                                            "cursor": 0, "event_cursor": 0})
                for e in r_all.get("events", []):
                    if e.get("event") == event_name:
                        return e
                return None
            await asyncio.sleep(0.1)
        return None


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


async def main():
    gw, gw_task, gc, bs, server = await setup()
    client = StudioClient()
    await asyncio.sleep(0.2)

    print("== M3 测试（交互闭环）==\n")

    # ── 1. clarify 全链路 ──
    gw.clarify_mode = True
    r = await client.request({"action": "chat", "session_id": "m3-cla", "message": "pick one"})
    run_id = r["run_id"]
    ev = await client.wait_event(run_id, "clarify.requested")
    check("clarify.事件出现", ev is not None, "未收到 clarify.requested")
    if ev:
        check("clarify.官方字段",
              all(k in ev for k in ("clarify_id", "question", "choices", "timeout_ms"))
              and ev.get("question") == "Which database?", ev)
        rc = await client.request({"action": "clarify_respond",
                                   "clarify_id": ev["clarify_id"], "response": "postgres"})
        check("clarify.respond.ok", rc.get("ok") is True and rc.get("resolved") is True, rc)
        await asyncio.sleep(0.3)
        # gateway 收到 {answer}
        answers = [v for v in gw.received_srq_results.values() if v.get("answer") == "postgres"]
        check("clarify.gateway收到answer", len(answers) >= 1, gw.received_srq_results)
        f = await client.wait_done(run_id)
        check("clarify.流完成", f.get("done") is True, f.get("status"))
    gw.clarify_mode = False

    # 未知 clarify_id → KeyError
    r = await client.request({"action": "clarify_respond", "clarify_id": "ghost", "response": "x"})
    check("clarify.未知id=KeyError", r.get("ok") is False and r.get("error_type") == "KeyError", r)

    # ── 2. sudo → -32601 快速失败 ──
    await gc.call("test.sudo", {"session_id": "tui-any"})
    await asyncio.sleep(0.3)
    sudo_errors = [e for e in gw.received_srq_errors.values() if e.get("code") == -32601]
    check("sudo.回-32601", len(sudo_errors) >= 1, gw.received_srq_errors)

    # ── 3. request.cancel：撤回后反向映射清理，迟到应答 KeyError ──
    gw.approval_mode = True
    r = await client.request({"action": "chat", "session_id": "m3-cxl", "message": "will cancel"})
    run_id = r["run_id"]
    ev = await client.wait_event(run_id, "approval.requested")
    check("cancel.审批事件先出现", ev is not None, "未收到 approval.requested")
    approval_id = ev["approval_id"] if ev else "none"
    # 撤回该审批（mock 对真实 srq id 发 request.cancel）
    await gc.call("test.cancel_pending", {})
    await asyncio.sleep(0.3)
    # 迟到应答 → KeyError（反向映射已清）
    r = await client.request({"action": "approval_respond",
                              "approval_id": approval_id, "choice": "once"})
    check("cancel.迟到应答=KeyError", r.get("ok") is False and r.get("error_type") == "KeyError", r)
    # resolved 事件注入
    ev2 = await client.wait_event(run_id, "approval.resolved")
    check("cancel.resolved事件注入", ev2 is not None and "cancelled" in str(ev2.get("choice", "")), ev2)
    # 流仍完成（撤回不阻塞）
    f = await client.wait_done(run_id)
    check("cancel.流完成", f.get("done") is True, f.get("status"))
    gw.approval_mode = False

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