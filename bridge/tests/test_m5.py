#!/usr/bin/env python3
"""M5 测试：MCP 六项 / context_estimate / provider_credentials / command / model 切换。

运行：python3 tests/test_m5.py
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


async def main():
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
    client = StudioClient()
    await asyncio.sleep(0.2)

    print("== M5 测试（管理功能）==\n")

    # 先建一个会话（context_estimate 需要）
    r = await client.request({"action": "chat", "session_id": "m5-s", "message": "hi"})
    run_id = r["run_id"]
    cursor = event_cursor = 0
    for _ in range(60):
        r = await client.request({"action": "get_output", "run_id": run_id,
                                  "cursor": cursor, "event_cursor": event_cursor})
        cursor = r.get("cursor", cursor)
        event_cursor = r.get("event_cursor", event_cursor)
        if r.get("done"):
            break
        await asyncio.sleep(0.1)

    # ── MCP 六项 ──
    r = await client.request({"action": "mcp_list"})
    check("mcp_list.ok+servers", r.get("ok") is True and isinstance(r.get("servers"), list)
          and len(r["servers"]) == 2, r)
    check("mcp_list.total_tools", r.get("total_tools") == 3, r.get("total_tools"))

    r = await client.request({"action": "mcp_server_add", "name": "fetch",
                              "config": {"command": "npx", "args": ["-y", "mcp-fetch"]}})
    check("mcp_server_add", r.get("ok") is True and r.get("name") == "fetch", r)

    r = await client.request({"action": "mcp_server_update", "name": "fetch",
                              "config": {"command": "npx", "args": ["-y", "mcp-fetch@2"]}})
    check("mcp_server_update", r.get("ok") is True, r)

    r = await client.request({"action": "mcp_server_test", "name": "fetch"})
    check("mcp_server_test", r.get("ok") is True and isinstance(r.get("tools"), list), r)

    r = await client.request({"action": "mcp_tools_list", "server": "fetch"})
    check("mcp_tools_list", r.get("ok") is True and isinstance(r.get("tools"), list), r)

    r = await client.request({"action": "mcp_server_remove", "name": "fetch"})
    check("mcp_server_remove", r.get("ok") is True, r)

    r = await client.request({"action": "mcp_reload"})
    check("mcp_reload", r.get("ok") is True and "reloaded" in str(r.get("message", "")), r)

    r = await client.request({"action": "mcp_bogus_action"})
    check("mcp_未知action=ok:false", r.get("ok") is False, r)

    r = await client.request({"action": "mcp_server_add", "name": "", "config": {}})
    check("mcp_add.缺参数=ok:false", r.get("ok") is False, r)

    # ── context_estimate ──
    r = await client.request({"action": "context_estimate", "session_id": "m5-s",
                              "messages": []})
    check("context_estimate.ok", r.get("ok") is True, r)
    check("context_estimate.fixed_context_tokens", r.get("fixed_context_tokens") == 175, r)
    check("context_estimate.tool_count", r.get("tool_count") == 2, r)
    check("context_estimate.tool_names", r.get("tool_names") == ["terminal", "web_search"], r)
    r = await client.request({"action": "context_estimate", "session_id": "ghost"})
    check("context_estimate.未知session零值", r.get("ok") is True
          and r.get("fixed_context_tokens") == 0, r)

    # ── provider_credentials（降级）──
    r = await client.request({"action": "provider_credentials", "profile": "default",
                              "provider": "anthropic"})
    check("provider_credentials.降级形状", r.get("ok") is True and r.get("configured") is False
          and r.get("provider") == "anthropic", r)

    # ── command / switch_session_model / skills_reload ──
    r = await client.request({"action": "command", "session_id": "m5-s", "command": "/status"})
    check("command.ok", r.get("ok") is True, r)
    r = await client.request({"action": "switch_session_model", "session_id": "m5-s",
                              "model": "glm-5.2", "provider": "zai"})
    check("switch_session_model.ok", r.get("ok") is True, r)
    r = await client.request({"action": "switch_session_model", "session_id": "m5-s", "model": ""})
    check("switch_session_model.缺model报错", r.get("ok") is False, r)
    r = await client.request({"action": "skills_reload"})
    check("skills_reload.ok", r.get("ok") is True, r)

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