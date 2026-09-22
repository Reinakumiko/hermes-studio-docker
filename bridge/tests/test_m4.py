#!/usr/bin/env python3
"""M4 测试：会话映射持久化 / 启动对账 / destroy_profile / turn 计数 / 重启恢复。

运行：python3 tests/test_m4.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
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


async def start_mock():
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
    return gw, gw_task


async def main():
    state_dir = tempfile.mkdtemp(prefix="bridge-m4-")
    state_file = os.path.join(state_dir, "state.json")

    gw, gw_task = await start_mock()
    gc = GatewayClient(f"ws://127.0.0.1:{GW_PORT}/api/ws")
    gc.state_file = state_file
    gc.load_state()
    await gc.connect()
    bs = BridgeServer(gc)
    server = await asyncio.start_server(bs.handle, "127.0.0.1", BRIDGE_PORT)
    client = StudioClient()
    await asyncio.sleep(0.2)

    print("== M4 测试（会话生命周期）==\n")

    # ── 1. 映射持久化：chat 后状态文件落盘 ──
    r = await client.request({"action": "chat", "session_id": "m4-a", "message": "hi",
                              "profile": "p1"})
    await client.wait_done(r["run_id"])
    check("持久化.文件存在", os.path.exists(state_file), state_file)
    with open(state_file) as f:
        state = json.load(f)
    check("持久化.映射落盘", state.get("sessions", {}).get("m4-a") == gc.sessions.get("m4-a"), state)
    check("持久化.profile落盘",
          state.get("profiles", {}).get(gc.sessions["m4-a"]) == "p1", state.get("profiles"))

    # ── 2. turn 计数：status.message_count ──
    r2 = await client.request({"action": "chat", "session_id": "m4-a", "message": "again"})
    await client.wait_done(r2["run_id"])
    st = await client.request({"action": "status", "session_id": "m4-a"})
    check("计数.两turn后=2", st.get("message_count") == 2, st.get("message_count"))

    # ── 3. destroy_profile：只关指定 profile ──
    r = await client.request({"action": "chat", "session_id": "m4-b", "message": "yo",
                              "profile": "p2"})
    await client.wait_done(r["run_id"])
    check("profile.两个会话存在", len(gc.sessions) == 2, gc.sessions)
    d = await client.request({"action": "destroy_profile", "profile": "p1"})
    check("profile.销毁ok", d.get("ok") is True, d)
    check("profile.只剩p2会话",
          list(gc.sessions) == ["m4-b"] and gc.session_profiles.get(gc.sessions["m4-b"]) == "p2",
          gc.sessions)
    st = await client.request({"action": "status", "session_id": "m4-a"})
    check("profile.p1会话已不存在", st.get("exists") is False, st)

    # ── 4. 模拟 bridge 重启：新 GatewayClient + load_state + reconcile → 映射恢复且复用 ──
    sid_before = gc.sessions["m4-b"]
    server.close()
    await gc.close()

    gc2 = GatewayClient(f"ws://127.0.0.1:{GW_PORT}/api/ws")
    gc2.state_file = state_file
    restored = gc2.load_state()
    await gc2.connect()
    await gc2.reconcile()
    check("重启.映射恢复", restored == 1 and gc2.sessions.get("m4-b") == sid_before,
          (restored, gc2.sessions))
    bs2 = BridgeServer(gc2)
    server = await asyncio.start_server(bs2.handle, "127.0.0.1", BRIDGE_PORT)
    await asyncio.sleep(0.2)

    mock_session_count_before = len(gw.sessions)
    r = await client.request({"action": "chat", "session_id": "m4-b", "message": "after restart"})
    await client.wait_done(r["run_id"])
    check("重启.复用会话(无新建)",
          len(gw.sessions) == mock_session_count_before, (len(gw.sessions), mock_session_count_before))
    check("重启.tui映射一致", gc2.sessions["m4-b"] == sid_before, gc2.sessions)

    # ── 5. 对账：stale tui_sid 被丢弃 ──
    # 手动注入一条 stale 映射（gateway 侧不存在 tui-session-999）
    gc2.sessions["m4-ghost"] = "tui-session-999"
    gc2.tui_sessions["tui-session-999"] = "m4-ghost"
    kept = await gc2.reconcile()
    check("对账.stale被丢弃", "m4-ghost" not in gc2.sessions and kept == 1, gc2.sessions)

    # ── 清理 ──
    server.close()
    gw_task.cancel()
    await gc2.close()
    import shutil
    shutil.rmtree(state_dir, ignore_errors=True)

    print(f"\n结果: {passed} 通过, {failed} 失败")
    if failures:
        print("\n失败明细:")
        for f in failures:
            print(f"  - {f}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())