#!/usr/bin/env python3
"""契约测试套件（M1.6）：对照官方 bridge_server.py / bridge_pool.py 的响应格式逐字段断言。

运行：python3 tests/test_contract.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mock_gateway import MockGateway          # noqa: E402
from bridge import GatewayClient, BridgeServer  # noqa: E402

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
    """模拟官方 hermes-studio bridge 客户端：一连接一请求。"""

    def __init__(self):
        self.port = BRIDGE_PORT

    async def request(self, req: dict, timeout: float = 10.0) -> dict:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
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


async def wait_run_done(client: StudioClient, run_id: str, max_s: float = 10.0) -> dict:
    """模拟 studio streamOutput 轮询直到 done。"""
    cursor = 0
    event_cursor = 0
    last = {}
    for _ in range(int(max_s / 0.1)):
        last = await client.request(
            {"action": "get_output", "run_id": run_id,
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


async def main():
    gw, gw_task, gc, bs, server = await setup()
    client = StudioClient()
    await asyncio.sleep(0.2)

    print("== M1.6 契约测试（对照官方格式）==\n")

    # ── 1. ping 官方形状 ──
    r = await client.request({"action": "ping"})
    check("ping.ok", r.get("ok") is True, r)
    check("ping.官方字段",
          all(k in r for k in ("pong", "time", "pid", "agent_root", "profile",
                               "hermes_home", "session_count", "running_session_count"))
          and r.get("pong") is True, r)

    # ── 2. chat 官方形状：{run_id, session_id, status} ──
    r = await client.request({"action": "chat", "session_id": "s1", "message": "Hello"})
    check("chat.ok", r.get("ok") is True, r)
    check("chat.三字段", all(k in r for k in ("run_id", "session_id", "status")), r)
    check("chat.session_id回传", r.get("session_id") == "s1", r)
    check("chat.status=running", r.get("status") == "running", r)
    run_id = r.get("run_id")
    check("chat.run_id非空", bool(run_id), r)

    # ── 3. get_output 官方全字段 + 分块游标 ──
    final = await wait_run_done(client, run_id)
    check("get_output.done", final.get("done") is True, final)
    check("get_output.官方10字段",
          all(k in final for k in ("run_id", "session_id", "status", "delta", "cursor",
                                    "output", "done", "result", "error", "events",
                                    "event_cursor")), final)
    check("get_output.output全量", "Mock reply to: Hello" in final.get("output", ""), final.get("output"))
    check("get_output.result完成", final.get("result") == "Mock reply to: Hello", final.get("result"))
    check("get_output.status=completed", final.get("status") == "completed", final.get("status"))
    # 分块游标：cursor 应为块数（>1），且增量拼接 == output
    check("get_output.cursor是块数", isinstance(final.get("cursor"), int) and final["cursor"] >= 1,
          final.get("cursor"))

    # 增量语义：cursor=0 拿全量，cursor=块数 拿空
    r0 = await client.request({"action": "get_output", "run_id": run_id, "cursor": 0, "event_cursor": 0})
    check("get_output.cursor=0拿全量", r0.get("delta") == final.get("output"), r0.get("delta"))
    rn = await client.request({"action": "get_output", "run_id": run_id,
                               "cursor": final["cursor"], "event_cursor": final["event_cursor"]})
    check("get_output.cursor=末尾拿空", rn.get("delta") == "", rn.get("delta"))

    # ── 4. 工具事件官方字段 ──
    r_all = await client.request({"action": "get_output", "run_id": run_id, "cursor": 0, "event_cursor": 0})
    events = r_all.get("events", [])
    tool_started = [e for e in events if e.get("event") == "tool.started"]
    tool_completed = [e for e in events if e.get("event") == "tool.completed"]
    check("事件.tool.started存在", len(tool_started) == 1, events)
    if tool_started:
        check("事件.tool.started官方字段",
              all(k in tool_started[0] for k in ("tool_call_id", "tool_name", "args"))
              and tool_started[0].get("tool_name") == "terminal", tool_started[0])
    check("事件.tool.completed存在", len(tool_completed) == 1, events)
    if tool_completed:
        check("事件.tool.completed官方字段",
              all(k in tool_completed[0] for k in ("tool_call_id", "tool_name", "args", "result")),
              tool_completed[0])

    # ── 5. get_result 官方形状 ──
    r = await client.request({"action": "get_result", "run_id": run_id})
    check("get_result.官方字段",
          all(k in r for k in ("run_id", "session_id", "status", "started_at", "ended_at",
                               "output", "deltas", "events", "result", "error")), r)
    check("get_result.deltas是列表", isinstance(r.get("deltas"), list), type(r.get("deltas")))

    # ── 6. 错误语义（官方：unknown action → ValueError；unknown run → KeyError）──
    r = await client.request({"action": "totally_unknown_xyz"})
    check("未知action.ok=false", r.get("ok") is False, r)
    check("未知action.error_type=ValueError", r.get("error_type") == "ValueError", r)
    check("未知action.error含action名", "totally_unknown_xyz" in str(r.get("error", "")), r)

    r = await client.request({"action": "get_output", "run_id": "nope", "cursor": 0, "event_cursor": 0})
    check("未知run.ok=false", r.get("ok") is False, r)
    check("未知run.error_type=KeyError", r.get("error_type") == "KeyError", r)

    r = await client.request({"action": ""})
    check("空action.ok=false", r.get("ok") is False and r.get("error_type") == "ValueError", r)

    # ── 7. status / list 官方形状 ──
    r = await client.request({"action": "status", "session_id": "s1"})
    check("status.已知会话", r.get("exists") is True and r.get("session_id") == "s1"
          and "current_run_id" in r, r)
    r = await client.request({"action": "status", "session_id": "ghost"})
    check("status.未知会话exists=false",
          r.get("exists") is False and r.get("running") is False and "message_count" in r, r)

    r = await client.request({"action": "list"})
    check("list.sessions数组", isinstance(r.get("sessions"), list), r)
    if r.get("sessions"):
        s0 = r["sessions"][0]
        check("list.官方字段",
              all(k in s0 for k in ("session_id", "running", "current_run_id", "boundary_interrupt"))
              and s0["boundary_interrupt"].get("supported") is False, s0)

    # ── 8. get_history / get_session_title ──
    r = await client.request({"action": "get_history", "session_id": "s1"})
    check("get_history.ok含history", r.get("ok") is True and "history" in r
          and r.get("session_id") == "s1", r)
    r = await client.request({"action": "get_history", "session_id": "ghost"})
    check("get_history.未知会话KeyError", r.get("ok") is False and r.get("error_type") == "KeyError", r)

    r = await client.request({"action": "get_session_title", "session_id": "s1"})
    check("get_session_title形状", r.get("ok") is True and r.get("session_id") == "s1"
          and isinstance(r.get("title"), str), r)

    # ── 9. 审批闭环：srq → approval.requested 事件 → approval_respond → gateway 收到应答 ──
    gw.approval_mode = True
    r = await client.request({"action": "chat", "session_id": "s2", "message": "need approval"})
    run2 = r.get("run_id")

    # 轮询直到出现 approval.requested
    approval_event = None
    event_cursor = 0
    for _ in range(50):
        rr = await client.request({"action": "get_output", "run_id": run2,
                                   "cursor": 0, "event_cursor": event_cursor})
        event_cursor = rr.get("event_cursor", event_cursor)
        approval_event = next((e for e in rr.get("events", [])
                               if e.get("event") == "approval.requested"), None)
        if approval_event:
            break
        await asyncio.sleep(0.1)
    check("审批.事件出现", approval_event is not None, "未收到 approval.requested")
    if approval_event:
        check("审批.官方字段",
              all(k in approval_event for k in ("run_id", "approval_id", "command",
                                                "description", "choices", "allow_permanent",
                                                "timeout_ms"))
              and approval_event.get("command") == "rm -rf /tmp/x", approval_event)
        approval_id = approval_event["approval_id"]

        r = await client.request({"action": "approval_respond",
                                  "approval_id": approval_id, "choice": "once"})
        check("审批.respond.ok", r.get("ok") is True and r.get("resolved") is True, r)

        # gateway 侧应收到 {choice, all}
        await asyncio.sleep(0.3)
        got = gw.received_srq_results.get("srq-1")
        check("审批.gateway收到应答", got is not None, gw.received_srq_results)
        if got:
            check("审批.应答形状", got.get("choice") == "once" and got.get("all") is False, got)

        # 流应继续并完成（含 approval.resolved 事件）
        final2 = await wait_run_done(client, run2)
        check("审批.审批后流完成", final2.get("done") is True, final2.get("status"))
        r_all2 = await client.request({"action": "get_output", "run_id": run2,
                                       "cursor": 0, "event_cursor": 0})
        resolved = [e for e in r_all2.get("events", []) if e.get("event") == "approval.resolved"]
        check("审批.resolved事件", len(resolved) == 1 and resolved[0].get("choice") == "once", resolved)

    # 未知 approval_id → KeyError
    r = await client.request({"action": "approval_respond", "approval_id": "ghost", "choice": "once"})
    check("审批.未知id=KeyError", r.get("ok") is False and r.get("error_type") == "KeyError", r)

    # ── 10. 一连接一请求：同连接第二个请求无响应（EOF）──
    reader, writer = await asyncio.open_connection("127.0.0.1", BRIDGE_PORT)
    writer.write(b'{"action":"ping"}\n')
    await writer.drain()
    line1 = await asyncio.wait_for(reader.readline(), timeout=5)
    check("一连接一请求.第一个有响应", b'"ok"' in line1, line1)
    writer.write(b'{"action":"ping"}\n')  # 第二个请求
    await writer.drain()
    line2 = await asyncio.wait_for(reader.readline(), timeout=3)
    check("一连接一请求.第二个无响应(连接关闭)", line2 == b"", line2)
    writer.close()

    # ── 11. shutdown：官方形状 + bridge 存活 ──
    r = await client.request({"action": "shutdown"})
    check("shutdown.官方形状", r.get("status") == "shutting_down" and "cleanup" in r, r)
    r = await client.request({"action": "ping"})
    check("shutdown.bridge仍存活", r.get("ok") is True, r)

    # ── 12. chat wait:true ──
    gw.approval_mode = False  # 该用例不测审批
    r = await client.request({"action": "chat", "session_id": "s3", "message": "sync",
                              "wait": True, "timeout": 10})
    check("chat.wait返回get_result形状",
          r.get("ok") is True and r.get("status") == "completed"
          and "Mock reply to: sync" in str(r.get("output", "")), r)

    # ── 13. 优雅降级 action 返回 ok ──
    for act, extra in [("background_poll", {}), ("goal_evaluate", {"session_id": "s1", "final_response": "x"}),
                       ("compression_respond", {"request_id": "r1"}), ("mcp_list", {})]:
        r = await client.request({"action": act, **extra})
        check(f"降级.{act}.ok", r.get("ok") is True, r)

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