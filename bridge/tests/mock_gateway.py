#!/usr/bin/env python3
"""Mock tui_gateway v2：模拟 hermes serve 的 WebSocket JSON-RPC 服务。

契约测试夹具。支持：
- session.create / prompt.submit（流式回复：tool.start → deltas → [approval srq] → tool.complete → message.complete）
- session.history / title / status / list / close / interrupt / steer
- command.dispatch / skills.reload
- server→client approval 请求（等待 bridge 应答后继续流式）
"""
from __future__ import annotations

import asyncio
import json
import logging

import websockets

log = logging.getLogger("mock-gateway")


class MockGateway:
    def __init__(self, port=19119):
        self.port = port
        self.sessions: dict[str, dict] = {}
        self._next_sid = 0
        # srq 应答协调：srq_id -> (Event, 应答结果)
        self.srq_replies: dict[str, tuple[asyncio.Event, dict]] = {}
        self.received_srq_results: dict[str, dict] = {}  # 记录 bridge 的应答（断言用）
        self.approval_mode = False  # prompt.submit 流程中是否插入审批
        self._next_srq = 0
        # M2：busy 行为（prompt.submit 时该 session 已有流进行中）
        #   "steer" → 返回 {"status":"steered"}，新文本并入当前流（追加 delta）
        #   "queue" → 返回 {"status":"queued"}，当前流完成后顺序执行
        self.busy_mode = "steer"
        self.fail_mode = False      # message.complete 带 error → run failed
        self.clarify_mode = False   # 流中插入 clarify srq
        self._active_streams: dict[str, asyncio.Task] = {}
        self._queued_prompts: dict[str, list[str]] = {}
        self.received_srq_errors: dict[str, dict] = {}   # bridge 的 -32601 等错误应答
        self.last_approval_srq: str | None = None       # 最近一次审批 srq 的 id（cancel 测试用）
        # M6：seq / 回放 / 心跳 / kick
        self.ready_heartbeat = False   # gateway.ready 声明心跳支持
        self.ping_count = 0
        self.connection_count = 0
        self.since_calls: list[tuple[str, int]] = []    # (session_id, last_seen) 记录
        self._seq: dict[str, int] = {}                  # tui_sid -> 事件 seq 计数
        self.event_log: dict[str, list[dict]] = {}       # tui_sid -> 已发事件（回放源）

    async def handle(self, ws):
        self.connection_count += 1
        log.info("客户端连接 #%d: %s", self.connection_count, ws.remote_address)
        try:
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "method": "gateway.ready",
                "params": {"type": "gateway.ready", "version": "0.21.0",
                           "heartbeat": self.ready_heartbeat}}))
            async for raw in ws:
                msg = json.loads(raw)
                rid = msg.get("id")
                # bridge 对 server→client 请求的应答（srq-* id 带 result 或 error）
                if isinstance(rid, str) and rid.startswith("srq-"):
                    if "result" in msg:
                        self.received_srq_results[rid] = msg["result"]
                        coord = self.srq_replies.get(rid)
                        if coord:
                            coord[0].set()
                        continue
                    if "error" in msg:
                        self.received_srq_errors[rid] = msg["error"]
                        coord = self.srq_replies.get(rid)
                        if coord:
                            coord[0].set()
                        continue
                await self.dispatch(ws, msg)
        except Exception as e:
            log.info("连接关闭: %s", e)

    async def dispatch(self, ws, msg):
        method = msg.get("method")
        rid = msg.get("id")
        params = msg.get("params") or {}
        log.info("RPC: %s %s", method, params)

        if method == "client.capabilities":
            result = {"server_requests": True, "methods": ["approval", "clarify"]}
        elif method == "ping":
            self.ping_count += 1
            result = {"pong": True}
        elif method == "session.create":
            self._next_sid += 1
            sid = f"tui-session-{self._next_sid}"
            self.sessions[sid] = {"messages": params.get("messages") or [], "title": "",
                                  "profile": params.get("profile")}
            result = {"session_id": sid, "stored_session_id": sid,
                      "message_count": len(self.sessions[sid]["messages"]),
                      "messages": self.sessions[sid]["messages"], "info": {}}
        elif method == "prompt.submit":
            sid = params.get("session_id")
            text = params.get("text", "")
            self.sessions.setdefault(sid, {"messages": [], "title": ""})
            self.sessions[sid]["messages"].append({"role": "user", "text": text})
            active = self._active_streams.get(sid)
            if active is not None and not active.done():
                if self.busy_mode == "steer":
                    result = {"status": "steered"}
                    asyncio.create_task(self._steer_current(ws, sid, text))
                else:  # queue
                    result = {"status": "queued"}
                    self._queued_prompts.setdefault(sid, []).append(text)
            else:
                result = {"status": "streaming"}
                self._start_stream(ws, sid, text)
            log.info("prompt.submit %s -> %s", text[:20], result.get("status"))
        elif method == "session.interrupt":
            result = {"ok": True}
        elif method == "session.steer":
            result = {"ok": True}
        elif method == "session.list":
            result = {"sessions": [{"session_id": k, "title": v.get("title", "")}
                                   for k, v in self.sessions.items()]}
        elif method == "session.history":
            sid = params.get("session_id")
            msgs = self.sessions.get(sid, {}).get("messages", [])
            result = {"count": len(msgs), "messages": msgs}
        elif method == "session.title":
            sid = params.get("session_id")
            result = {"title": self.sessions.get(sid, {}).get("title", "")}
        elif method == "session.status":
            result = {"session_id": params.get("session_id"), "status": "idle"}
        elif method == "session.close":
            self.sessions.pop(params.get("session_id"), None)
            result = {"ok": True}
        elif method == "session.context_breakdown":
            result = {"system_tokens": 100, "tool_tokens": 50, "history_tokens": 25,
                      "total": 175, "tool_count": 2, "tool_names": ["terminal", "web_search"]}
        elif method == "mcp.servers.list":
            result = {"servers": [{"name": "fetch", "tools": 2, "status": "connected"},
                                  {"name": "memory", "tools": 1, "status": "connected"}]}
        elif method == "mcp.servers.add":
            result = {"ok": True}
        elif method == "mcp.servers.remove":
            result = {"ok": True}
        elif method == "mcp.servers.test":
            result = {"ok": True, "tools": ["fetch_page", "search"]}
        elif method == "mcp.servers.status":
            result = {"ok": True, "tools": ["fetch_page", "search"], "status": "connected"}
        elif method == "reload.mcp":
            result = {"ok": True}
        elif method == "session.events.since":
            sid = params.get("session_id")
            last = int(params.get("last_seen") or 0)
            self.since_calls.append((sid, last))
            events = [e for e in self.event_log.get(sid, []) if e.get("seq", 0) > last]
            result = {"events": events, "latest_seq": self._seq.get(sid, 0),
                      "truncated": False, "count": len(events), "epoch": "mock"}
        elif method == "test.kick":
            # 测试辅助：延迟关闭当前 WS 连接（模拟 gateway 断线）
            async def _close():
                await asyncio.sleep(0.1)
                await ws.close(code=1011)
            asyncio.create_task(_close())
            result = {"ok": True}
        elif method == "command.dispatch":
            result = {"ok": True, "dispatched": params.get("command")}
        elif method == "skills.reload":
            result = {"ok": True}
        elif method == "test.emit":
            # 测试辅助：对指定 session 直接发 delta+complete（模拟后台 turn，无 chat 先行）
            sid = params.get("session_id")
            await self._send_event(ws, {"type": "message.delta", "session_id": sid, "text": "bg text"})
            await self._send_event(ws, {"type": "message.complete", "session_id": sid, "text": "bg text"})
            result = {"ok": True}
        elif method == "test.cancel_pending":
            # 测试辅助：撤回最近一次审批 srq（request.cancel 事件）
            if self.last_approval_srq:
                await self._send_event(ws, {"type": "request.cancel", "id": self.last_approval_srq,
                                            "method": "approval", "reason": "timeout"})
            result = {"ok": True}
        elif method == "test.sudo":
            # 测试辅助：发 sudo server→client 请求（bridge 应回 -32601）
            self._next_srq += 1
            srq_id = f"srq-{self._next_srq}"
            ev = asyncio.Event()
            self.srq_replies[srq_id] = (ev, {})
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": srq_id, "method": "sudo",
                                      "params": {"session_id": params.get("session_id"),
                                                 "command": "rm -rf /"}}))
            try:
                await asyncio.wait_for(ev.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass
            result = {"ok": True}
        elif method == "test.cancel":
            # 测试辅助：发 approval srq 后立即发 request.cancel 撤回
            self._next_srq += 1
            srq_id = f"srq-{self._next_srq}"
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": srq_id, "method": "approval",
                                      "params": {"session_id": params.get("session_id"),
                                                 "command": "x", "description": "y",
                                                 "choices": ["once", "deny"],
                                                 "allow_permanent": False}}))
            await asyncio.sleep(0.1)
            await self._send_event(ws, {"type": "request.cancel", "id": srq_id,
                                         "method": "approval", "reason": "timeout"})
            result = {"ok": True}
        else:
            result = {"ok": True}

        await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}))

    async def _send_event(self, ws, params):
        # M6：带 session_id 的事件自动编号 seq 并记录回放日志
        sid = params.get("session_id")
        if sid:
            seq = self._seq.get(sid, 0) + 1
            self._seq[sid] = seq
            params = {**params, "seq": seq}
            self.event_log.setdefault(sid, []).append(dict(params))
        await ws.send(json.dumps({"jsonrpc": "2.0", "method": "event", "params": params}))

    async def _steer_current(self, ws, sid, text):
        """busy_mode=steer：把新文本作为追加 delta 并入当前流。"""
        await self._send_event(ws, {"type": "message.delta", "session_id": sid,
                                    "text": f" [steered: {text}]"})

    def _start_stream(self, ws, sid, text):
        task = asyncio.create_task(self._stream_reply(ws, sid, text))
        self._active_streams[sid] = task
        return task

    async def _stream_reply(self, ws, sid, text):
        """模拟 agent turn：tool.start → deltas → [approval srq] → tool.complete → message.complete"""
        try:
            reply = f"Mock reply to: {text}"
            tool_id = "call-1"

            await self._send_event(ws, {"type": "tool.start", "session_id": sid,
                                        "tool_id": tool_id, "name": "terminal",
                                        "args": {"command": "ls"}, "args_text": "ls", "preview": None})
            await asyncio.sleep(0.05)

            chunks = [reply[i:i + 10] for i in range(0, len(reply), 10)]
            for i, chunk in enumerate(chunks):
                await self._send_event(ws, {"type": "message.delta", "session_id": sid, "text": chunk})
                await asyncio.sleep(0.05)
                # 第一块后插入审批/澄清请求（对应 mode 开启时）
                if i == 0:
                    if self.approval_mode:
                        await self._request_approval(ws, sid)
                    if self.clarify_mode:
                        await self._request_clarify(ws, sid)

            await self._send_event(ws, {"type": "tool.complete", "session_id": sid,
                                        "tool_id": tool_id, "name": "terminal",
                                        "args": {"command": "ls"}, "duration_s": 0.1,
                                        "result": {"exit_code": 0}, "summary": "listed files",
                                        "result_text": "README.md src/"})
            if self.fail_mode:
                await self._send_event(ws, {"type": "message.complete", "session_id": sid,
                                            "text": reply, "error": "mock agent failure"})
            else:
                await self._send_event(ws, {"type": "message.complete", "session_id": sid,
                                            "text": reply, "usage": {"total_tokens": 42},
                                            "status": "completed"})
            await self._send_event(ws, {"type": "session.title", "session_id": sid,
                                        "title": "mock conversation"})
        finally:
            self._active_streams.pop(sid, None)
            # queue 模式：顺序执行下一个排队 prompt
            queued = self._queued_prompts.get(sid)
            if queued:
                next_text = queued.pop(0)
                if not queued:
                    self._queued_prompts.pop(sid, None)
                self._start_stream(ws, sid, next_text)

    async def _request_approval(self, ws, sid):
        self._next_srq += 1
        srq_id = f"srq-{self._next_srq}"
        self.last_approval_srq = srq_id
        ev = asyncio.Event()
        self.srq_replies[srq_id] = (ev, {})
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": srq_id, "method": "approval",
            "params": {"session_id": sid, "request_id": "req-1",
                       "command": "rm -rf /tmp/x", "description": "test approval",
                       "choices": ["once", "session", "always", "deny"],
                       "allow_permanent": True}}))
        # 等 bridge 应答（最多 5s），否则视为超时 deny
        try:
            await asyncio.wait_for(ev.wait(), timeout=5)
        except asyncio.TimeoutError:
            log.warning("审批应答超时")

    async def _request_clarify(self, ws, sid):
        self._next_srq += 1
        srq_id = f"srq-{self._next_srq}"
        ev = asyncio.Event()
        self.srq_replies[srq_id] = (ev, {})
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": srq_id, "method": "clarify",
            "params": {"session_id": sid, "question": "Which database?",
                       "choices": ["postgres", "sqlite"]}}))
        try:
            await asyncio.wait_for(ev.wait(), timeout=5)
        except asyncio.TimeoutError:
            log.warning("澄清应答超时")

    async def run(self):
        async with websockets.serve(self.handle, "127.0.0.1", self.port):
            log.info("mock gateway 监听 ws://127.0.0.1:%s", self.port)
            await asyncio.Future()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(MockGateway().run())