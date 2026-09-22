#!/usr/bin/env python3
"""
hermes-studio ↔ hermes gateway 桥接服务 v0.2（M1：协议保真）
=============================================================
让 hermes-studio 作为纯客户端连接已运行的 hermes gateway（hermes serve / tui_gateway）。

架构：
    hermes-studio ──(bridge协议: TCP JSON Lines, 一连接一请求)──> 本 bridge
    本 bridge ──(tui_gateway JSON-RPC over WebSocket /api/ws)──> hermes serve

M1 变更（对照官方 bridge_server.py / bridge_pool.py 逐字段对齐，依据 PLAN.md 附录 B）：
- chat 响应: {run_id, session_id, status}；支持 wait:true 阻塞语义
- get_output: 官方全字段；deltas 为分块列表，cursor=块数（非字符数）；未知 run → ok:false
- get_result: 官方全字段
- 错误响应: {"ok": false, "error": str, "error_type": 类名}
- 一连接一请求（对齐官方 serve_forever）
- 事件格式: tool.started/tool.completed/approval.requested 等官方字段名

有意偏离官方（见 PLAN.md）：
- shutdown 不退出 bridge（本 bridge 是常驻共享服务，非 studio 子进程）
- goal_*/compression_respond/background_* 等无 tui 对应物 → 优雅降级返回 ok

用法：
    python3 bridge.py --listen 0.0.0.0:18765 --gateway ws://127.0.0.1:9119/api/ws [--token X]

依赖：pip install websockets
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
import uuid

import websockets

log = logging.getLogger("hermes-bridge")

APPROVAL_TIMEOUT_MS = 120_000   # 事件里告知 studio 的审批超时
CLARIFY_TIMEOUT_MS = 300_000
REQUEST_READ_TIMEOUT = 120.0    # 单连接读请求超时
RPC_TIMEOUT = 120.0


# ─────────────────────────── Run 模型（对齐官方 bridge_pool） ───────────────────────────

class RunRecord:
    """官方语义：deltas 是分块列表；cursor = len(deltas)（块数）；delta = 块切片拼接。"""

    def __init__(self, run_id: str, session_id: str):
        self.run_id = run_id
        self.session_id = session_id            # studio 侧 session_id
        self.tui_session_id: str | None = None  # gateway 侧 session_id
        self.status = "running"
        self.deltas: list[str] = []
        self.events: list[dict] = []
        self.result = None
        self.error = None
        self.started_at = time.time()
        self.ended_at: float | None = None

    def finish(self, status: str, result=None, error=None):
        if self.status != "running":
            return
        self.status = status
        self.result = result
        self.error = error
        self.ended_at = time.time()

    def append_event(self, event: dict):
        self.events.append(event)

    def output_view(self, cursor: int = 0, event_cursor: int = 0) -> dict:
        """官方 bridge_pool.get_output 逐字段对齐。"""
        cursor = max(0, int(cursor or 0))
        event_cursor = max(0, int(event_cursor or 0))
        return {
            "run_id": self.run_id,
            "session_id": self.session_id,
            "status": self.status,
            "delta": "".join(self.deltas[cursor:]),
            "cursor": len(self.deltas),
            "output": "".join(self.deltas),
            "done": self.status != "running",
            "result": self.result if self.status != "running" else None,
            "error": self.error,
            "events": self.events[event_cursor:],
            "event_cursor": len(self.events),
        }

    def result_view(self) -> dict:
        """官方 bridge_pool.get_result 逐字段对齐。"""
        return {
            "run_id": self.run_id,
            "session_id": self.session_id,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "output": "".join(self.deltas),
            "deltas": list(self.deltas),
            "events": list(self.events),
            "result": self.result,
            "error": self.error,
        }


# ─────────────────────────── Gateway 客户端（Desktop 同款连接方式） ───────────────────────────

class GatewayClient:
    """tui_gateway JSON-RPC WebSocket 客户端。

    连接方式与官方 Desktop JsonRpcGatewayClient 一致：
    - ws://host:port/api/ws（?token= 认证）
    - 换行分隔 JSON-RPC 2.0
    - gateway.ready 后发 client.capabilities {server_requests: true}
    """

    def __init__(self, url: str, token: str = ""):
        self.url = url
        self.token = token
        self.ws = None
        self._next_id = 0
        self._pending: dict[str, asyncio.Future] = {}
        self._ready = asyncio.Event()
        self._recv_task: asyncio.Task | None = None

        # 会话映射：studio_sid ↔ tui_sid
        self.sessions: dict[str, str] = {}         # studio_sid -> tui_sid
        self.tui_sessions: dict[str, str] = {}      # tui_sid -> studio_sid
        self.session_titles: dict[str, str] = {}   # tui_sid -> title
        self.session_profiles: dict[str, str | None] = {}  # tui_sid -> profile
        self.session_turns: dict[str, int] = {}    # tui_sid -> 完成 turn 数（status.message_count）
        self.state_file: str | None = None         # M4：映射持久化路径

        # run 注册表
        self.runs: dict[str, RunRecord] = {}
        self._studio_session_runs: dict[str, list[str]] = {}
        # run GC 配置（M2.5）
        self.run_retain_s = 1800.0   # 完成后保留时长
        self.max_runs = 1000         # 注册表上限（超出逐出最旧已完成 run）

        # server→client 请求（approval/clarify）
        self.pending_srq: dict[str, dict] = {}     # srq_id -> {method, params, run_id}
        self.approval_map: dict[str, str] = {}    # approval_id -> srq_id
        self.clarify_map: dict[str, str] = {}     # clarify_id -> srq_id

        # M6：重连 / 回放 / 心跳
        self._closing = False
        self._connected_once = False
        self._ready_payload: dict = {}
        self.heartbeat_interval = 30.0             # gateway.ready.heartbeat 时生效
        self.reconnect_min_delay = 1.0
        self.reconnect_max_delay = 60.0
        self._heartbeat_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None  # 强引用防 GC（asyncio 弱引用陷阱）
        self.session_event_seq: dict[str, int] = {}  # tui_sid -> 最近事件 seq（回放水位）

    # ── 连接与 RPC ──

    async def connect(self):
        await self._connect_once()
        self._connected_once = True

    async def _connect_once(self):
        url = self.url
        if self.token:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}token={self.token}"
        self._ready.clear()
        self.ws = await websockets.connect(url)
        self._recv_task = asyncio.create_task(self._recv_loop())
        await asyncio.wait_for(self._ready.wait(), timeout=15)
        # 与官方 Desktop 一致：ready 后声明支持 server→client 请求
        await self.call("client.capabilities", {"server_requests": True})
        self._maybe_start_heartbeat()
        log.info("已连接 gateway %s", self.url)

    async def call(self, method: str, params: dict | None = None, timeout: float = RPC_TIMEOUT):
        self._next_id += 1
        rid = str(self._next_id)
        frame = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            frame["params"] = params
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        await self.ws.send(json.dumps(frame))
        return await asyncio.wait_for(fut, timeout=timeout)

    async def reply_srq(self, srq_id, result: dict):
        await self.ws.send(json.dumps({"jsonrpc": "2.0", "id": srq_id, "result": result}))

    async def reply_srq_error(self, srq_id, code: int = -32601, message: str = "method not implemented"):
        await self.ws.send(json.dumps(
            {"jsonrpc": "2.0", "id": srq_id, "error": {"code": code, "message": message}}))

    async def close(self):
        self._closing = True
        if self._reconnect_task:
            self._reconnect_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._recv_task:
            self._recv_task.cancel()
        if self.ws:
            await self.ws.close()

    # ── M6：重连 / 回放 / 心跳 ──

    async def _reconnect_loop(self):
        """指数退避重连；成功后重发 capabilities 并回放断线期间事件。"""
        delay = self.reconnect_min_delay
        while not self._closing:
            log.warning("gateway 连接断开，%.1fs 后重连", delay)
            await asyncio.sleep(delay)
            if self._closing:
                return
            try:
                await self._connect_once()
                await self._replay_events()
                log.info("gateway 重连成功（事件回放完成）")
                return
            except Exception as exc:
                log.warning("重连失败: %s", exc)
                delay = min(delay * 2, self.reconnect_max_delay)

    async def _replay_events(self):
        """断线期间事件回放：session.events.since {last_seen=seq 水位}。"""
        for tui_sid, last_seen in list(self.session_event_seq.items()):
            try:
                result = await self.call("session.events.since",
                                         {"session_id": tui_sid, "last_seen": last_seen})
                events = (result or {}).get("events") or []
                for ev in events:
                    if isinstance(ev, dict) and ev.get("type"):
                        self._handle_event(ev)
                if events:
                    log.info("回放 %d 条事件（session=%s）", len(events), tui_sid)
            except Exception:
                log.warning("事件回放失败 session=%s", tui_sid)

    def _maybe_start_heartbeat(self):
        """gateway.ready.heartbeat 声明支持时，周期 ping（官方 Desktop 同款）。"""
        if (self._ready_payload or {}).get("heartbeat") and self.heartbeat_interval > 0:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                try:
                    await self.call("ping", {}, timeout=10)
                except Exception:
                    return  # 连接异常交给 _recv_loop 的重连逻辑
        except asyncio.CancelledError:
            pass

    # ── 接收循环 ──

    async def _recv_loop(self):
        try:
            async for raw in self.ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                rid = msg.get("id")
                if rid is not None and rid in self._pending:
                    fut = self._pending.pop(rid)
                    if "error" in msg:
                        fut.set_exception(RuntimeError(str(msg["error"])))
                    else:
                        fut.set_result(msg.get("result"))
                    continue
                method = msg.get("method")
                if method == "gateway.ready":
                    self._ready_payload = msg.get("params") or {}
                    self._ready.set()
                elif method == "event":
                    self._handle_event(msg.get("params") or {})
                elif method is not None:
                    await self._handle_server_request(msg)
        except Exception:
            pass
        finally:
            # 断线：失败所有挂起 RPC，按需触发重连
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("gateway connection lost"))
            self._pending.clear()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
            if not self._closing and self._connected_once:
                # 强引用防 GC：事件循环对 task 只持弱引用，未保存引用的任务可能中途消失
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    # ── tui 事件 → 官方 bridge deltas/events ──

    def _handle_event(self, p: dict):
        etype = p.get("type")
        # request.cancel 无 session_id（RequestCancelPayload = {id, method, reason}），先于守卫处理
        if etype == "request.cancel":
            self._handle_request_cancel(p)
            return
        tui_sid = p.get("session_id")
        if not tui_sid:
            return
        # M6：seq 水位跟踪（事件带 seq 时记录，供断线回放）
        seq = p.get("seq")
        if isinstance(seq, int):
            self.session_event_seq[tui_sid] = seq
        run = self._active_run_for_tui(tui_sid)

        if etype == "message.delta":
            text = p.get("text") or ""
            if not text:
                return
            if run is None:
                run = self._implicit_run(tui_sid)
            run.deltas.append(text)
        elif etype == "message.complete":
            if run is None:
                run = self._implicit_run(tui_sid)
            self.session_turns[tui_sid] = self.session_turns.get(tui_sid, 0) + 1
            error = p.get("error")
            if error:
                run.finish("failed", error=str(error))
            else:
                run.finish("completed", result=p.get("text"))
        elif etype == "tool.start":
            if run is not None:
                run.append_event({
                    "event": "tool.started",
                    "tool_call_id": str(p.get("tool_id") or ""),
                    "tool_name": str(p.get("name") or ""),
                    "args": p.get("args") or {},
                })
        elif etype == "tool.generating":
            if run is not None:
                run.append_event({
                    "event": "tool.generating",
                    "tool_name": str(p.get("name") or ""),
                })
        elif etype == "tool.complete":
            if run is not None:
                run.append_event({
                    "event": "tool.completed",
                    "tool_call_id": str(p.get("tool_id") or ""),
                    "tool_name": str(p.get("name") or ""),
                    "args": p.get("args") or {},
                    "result": p.get("result_text") or p.get("summary") or "",
                })
        elif etype == "session.title":
            title = p.get("title")
            if title:
                self.session_titles[tui_sid] = str(title)


    def _handle_request_cancel(self, p: dict):
        """gateway 撤回某个 server→client 请求（超时/中断/他处已答）。"""
        rid = p.get("id")
        entry = self.pending_srq.pop(rid, None)
        if entry:
            # 清理反向映射：迟到的 approval_respond/clarify_respond 将得到 KeyError
            for mapping in (self.approval_map, self.clarify_map):
                for key, val in list(mapping.items()):
                    if val == rid:
                        del mapping[key]
            run = self.runs.get(entry.get("run_id") or "")
            if run:
                method = entry.get("method")
                reason = str(p.get("reason") or "")
                if method == "approval":
                    run.append_event({"event": "approval.resolved", "run_id": entry.get("run_id"),
                                      "approval_id": "?", "choice": f"cancelled ({reason})"})
                elif method == "clarify":
                    run.append_event({"event": "clarify.resolved", "clarify_id": "?"})

    # ── server→client 请求（approval/clarify/...）──

    async def _handle_server_request(self, msg: dict):
        method = msg.get("method")
        rid = msg.get("id")
        params = msg.get("params") or {}
        tui_sid = params.get("session_id")
        run = self._active_run_for_tui(tui_sid) if tui_sid else None

        if method == "approval":
            approval_id = uuid.uuid4().hex
            self.pending_srq[rid] = {"method": method, "params": params,
                                     "run_id": run.run_id if run else ""}
            self.approval_map[approval_id] = rid
            choices = params.get("choices") or (
                ["once", "session", "always", "deny"] if params.get("allow_permanent", True)
                else ["once", "session", "deny"])
            if run:
                run.append_event({
                    "event": "approval.requested",
                    "run_id": run.run_id,
                    "approval_id": approval_id,
                    "command": str(params.get("command") or ""),
                    "description": str(params.get("description") or ""),
                    "choices": choices,
                    "allow_permanent": bool(params.get("allow_permanent", True)),
                    "timeout_ms": APPROVAL_TIMEOUT_MS,
                })
            log.info("approval 请求 srq=%s -> approval_id=%s", rid, approval_id)
        elif method == "clarify":
            clarify_id = uuid.uuid4().hex
            self.pending_srq[rid] = {"method": method, "params": params,
                                     "run_id": run.run_id if run else ""}
            self.clarify_map[clarify_id] = rid
            if run:
                run.append_event({
                    "event": "clarify.requested",
                    "clarify_id": clarify_id,
                    "question": str(params.get("question") or ""),
                    "choices": params.get("choices"),
                    "timeout_ms": CLARIFY_TIMEOUT_MS,
                })
            log.info("clarify 请求 srq=%s -> clarify_id=%s", rid, clarify_id)
        else:
            # sudo/secret/connection/terminal.read 等：按契约回 -32601 快速失败
            await self.reply_srq_error(rid)
            log.debug("未实现的 server->client 请求 %s -> -32601", method)

    # ── run 查找/创建 ──

    def _active_run_for_tui(self, tui_sid) -> RunRecord | None:
        """FIFO：同 session 的事件流向最早创建的 running run（M2 多 turn 语义）。

        - 顺序 turn：run1 完成 → run2 创建 → 事件自然流向 run2
        - queued turn：run1 仍是最早 running → turn1 事件进 run1；run1 完成后 turn2 事件进 run2
        - steer/redirect：chat 处理器会显式结束旧 run（见 _finish_superseded）
        """
        for run in self.runs.values():  # dict 保持插入序 = 创建序
            if run.tui_session_id == tui_sid and run.status == "running":
                return run
        return None

    def _finish_superseded(self, tui_sid: str, exclude_run_id: str):
        """steer/redirect 时结束同 session 的旧活跃 run（避免 studio 端悬挂轮询）。"""
        for run in self.runs.values():
            if (run.tui_session_id == tui_sid and run.status == "running"
                    and run.run_id != exclude_run_id):
                run.finish("completed")

    def gc_runs(self):
        """M2.5：清理超期已完成 run；超过上限时逐出最旧已完成 run。"""
        now = time.time()
        expired = [rid for rid, r in self.runs.items()
                   if r.status != "running" and r.ended_at is not None
                   and now - r.ended_at > self.run_retain_s]
        for rid in expired:
            self._forget_run(rid)
        finished = [rid for rid, r in self.runs.items() if r.status != "running"]
        if len(self.runs) - len(expired) > self.max_runs:
            for rid in finished[:len(self.runs) - self.max_runs]:
                self._forget_run(rid)

    def _forget_run(self, run_id: str):
        run = self.runs.pop(run_id, None)
        if run:
            ids = self._studio_session_runs.get(run.session_id)
            if ids and run_id in ids:
                ids.remove(run_id)
                if not ids:
                    self._studio_session_runs.pop(run.session_id, None)

    # ── M4：会话映射持久化 / 启动对账 ──

    def save_state(self):
        """映射持久化（原子写）：studio↔tui 会话、标题、profile。"""
        if not self.state_file:
            return
        state = {
            "sessions": self.sessions,
            "titles": self.session_titles,
            "profiles": self.session_profiles,
        }
        tmp = f"{self.state_file}.tmp"
        os.makedirs(os.path.dirname(os.path.abspath(self.state_file)), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, self.state_file)

    def load_state(self):
        """启动时恢复映射（bridge 重启不丢会话映射）。"""
        if not self.state_file or not os.path.exists(self.state_file):
            return 0
        try:
            with open(self.state_file, encoding="utf-8") as f:
                state = json.load(f)
        except (json.JSONDecodeError, OSError):
            log.warning("状态文件损坏，忽略: %s", self.state_file)
            return 0
        self.sessions = {k: v for k, v in (state.get("sessions") or {}).items()}
        self.tui_sessions = {v: k for k, v in self.sessions.items()}
        self.session_titles = dict(state.get("titles") or {})
        self.session_profiles = dict(state.get("profiles") or {})
        log.info("恢复会话映射 %d 条（%s）", len(self.sessions), self.state_file)
        return len(self.sessions)

    async def reconcile(self) -> int:
        """启动对账：gateway 侧已不存在的 tui 会话 → 丢弃映射。返回保留数。"""
        try:
            result = await self.call("session.list", {})
        except Exception:
            log.warning("对账失败（session.list 不可用），保留全部映射")
            return len(self.sessions)
        live: set[str] = set()
        for s in (result or {}).get("sessions") or []:
            sid = s.get("session_id") if isinstance(s, dict) else None
            if sid:
                live.add(str(sid))
        stale = [tui for tui in self.tui_sessions if tui not in live]
        for tui in stale:
            studio = self.tui_sessions.pop(tui)
            self.sessions.pop(studio, None)
            self.session_titles.pop(tui, None)
            self.session_profiles.pop(tui, None)
            self.session_turns.pop(tui, None)
        if stale:
            log.info("对账：丢弃 %d 条过期映射", len(stale))
            self.save_state()
        return len(self.sessions)

    def _implicit_run(self, tui_sid) -> RunRecord:
        """无活跃 run 时收到消息事件（后台 turn）→ 隐式建 run。"""
        studio_sid = self.tui_sessions.get(tui_sid, tui_sid)
        run = RunRecord(f"run-{uuid.uuid4().hex[:12]}", studio_sid)
        run.tui_session_id = tui_sid
        self._register_run(run)
        return run

    def _register_run(self, run: RunRecord):
        self.runs[run.run_id] = run
        self._studio_session_runs.setdefault(run.session_id, []).append(run.run_id)

    def new_run(self, studio_sid: str, tui_sid: str) -> RunRecord:
        run = RunRecord(f"run-{uuid.uuid4().hex[:12]}", studio_sid)
        run.tui_session_id = tui_sid
        self._register_run(run)
        return run

    def latest_run_for_studio(self, studio_sid: str) -> RunRecord | None:
        ids = self._studio_session_runs.get(studio_sid)
        if not ids:
            return None
        return self.runs.get(ids[-1])


def _studio_history_to_tui(history: list) -> list:
    """studio conversation_history → tui session.create messages（尽力转换，M4 联调修正）。"""
    out = []
    for m in history:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role") or "user")
        text = m.get("text") if isinstance(m.get("text"), str) else m.get("content")
        if text is None:
            continue
        out.append({"role": role, "text": str(text)})
    return out


# ─────────────────────────── Bridge 服务端（官方协议） ───────────────────────────

class BridgeServer:
    """TCP JSON Lines 服务端：一连接一请求（对齐官方 serve_forever）。

    响应包装（对齐官方）：
        成功: {"ok": true, **action_result}
        异常: {"ok": false, "error": str, "error_type": 类名}
    """

    def __init__(self, gateway: GatewayClient):
        self.gateway = gateway

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=REQUEST_READ_TIMEOUT)
            if not line:
                return
            try:
                req = json.loads(line)
                log.info("请求 %s from %s", str(req.get("action") or "?"), peer)
                try:
                    data = await self._dispatch(req)
                    resp = {"ok": True, **data}
                except Exception as exc:
                    log.warning("请求失败 %s: %s", req.get("action"), exc)
                    resp = {"ok": False, "error": str(exc), "error_type": type(exc).__name__}
            except json.JSONDecodeError as exc:
                resp = {"ok": False, "error": f"invalid JSON: {exc}", "error_type": "ValueError"}
            writer.write((json.dumps(resp) + "\n").encode())
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionResetError, asyncio.IncompleteReadError):
            pass
        except Exception:
            log.exception("连接处理异常")
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _dispatch(self, req) -> dict:
        if not isinstance(req, dict):
            raise ValueError("request must be a JSON object")
        action = str(req.get("action") or "").strip()
        if not action:
            raise ValueError("action is required")
        if action.startswith("mcp_"):
            return await self._handle_mcp_action(action, req)
        handler = getattr(self, f"_action_{action}", None)
        if handler is None:
            raise ValueError(f"unknown action: {action}")
        return await handler(req)

    # ── MCP 管理（M5：对接 tui_gateway mcp.servers.* / reload.mcp）──

    async def _handle_mcp_action(self, action: str, req: dict) -> dict:
        gw = self.gateway
        if action == "mcp_list":
            result = await gw.call("mcp.servers.list", {})
            servers = (result or {}).get("servers") or []
            total_tools = sum(int(s.get("tools") or s.get("tool_count") or 0)
                              for s in servers if isinstance(s, dict))
            return {"ok": True, "servers": servers, "total_tools": total_tools}
        if action == "mcp_server_add":
            name = str(req.get("name") or "").strip()
            config = req.get("config") or {}
            if not name or not config:
                return {"ok": False, "error": "name and config are required"}
            await gw.call("mcp.servers.add", {"name": name, "config": config})
            return {"ok": True, "name": name}
        if action == "mcp_server_update":
            name = str(req.get("name") or "").strip()
            config = req.get("config") or {}
            if not name or not config:
                return {"ok": False, "error": "name and config are required"}
            # tui 无独立 update：add 即 upsert（联调确认，见 PLAN R1）
            await gw.call("mcp.servers.add", {"name": name, "config": config})
            return {"ok": True}
        if action == "mcp_server_remove":
            name = str(req.get("name") or "").strip()
            if not name:
                return {"ok": False, "error": "name is required"}
            await gw.call("mcp.servers.remove", {"name": name})
            return {"ok": True}
        if action == "mcp_server_test":
            name = str(req.get("name") or "").strip()
            if not name:
                return {"ok": False, "error": "name is required"}
            result = await gw.call("mcp.servers.test", {"name": name})
            tools = (result or {}).get("tools") or []
            return {"ok": True, "tools": tools}
        if action == "mcp_tools_list":
            name = str(req.get("server") or "").strip()
            result = await gw.call("mcp.servers.status", {"name": name} if name else {})
            tools = (result or {}).get("tools") or []
            return {"ok": True, "tools": tools, "results": result if req.get("raw") else None}
        if action == "mcp_reload":
            await gw.call("reload.mcp", {})
            return {"ok": True, "message": "MCP servers reloaded"}
        return {"ok": False, "error": f"unknown MCP action: {action}"}

    # ── 核心：ping / chat / get_output / get_result ──

    async def _action_ping(self, req):
        runs = list(self.gateway.runs.values())
        return {
            "pong": True,
            "time": time.time(),
            "pid": os.getpid(),
            "agent_root": "",
            "profile": "default",
            "hermes_home": "",
            "session_count": len(self.gateway.sessions),
            "running_session_count": sum(1 for r in runs if r.status == "running"),
        }

    async def _ensure_tui_session(self, studio_sid: str, req: dict) -> str:
        tui_sid = self.gateway.sessions.get(studio_sid)
        if tui_sid:
            return tui_sid
        params: dict = {}
        if req.get("profile"):
            params["profile"] = req["profile"]
        if req.get("model"):
            params["model"] = req["model"]
        if req.get("provider"):
            params["provider"] = req["provider"]
        history = req.get("conversation_history")
        if isinstance(history, list) and history:
            params["messages"] = _studio_history_to_tui(history)
        result = await self.gateway.call("session.create", params)
        tui_sid = result.get("session_id")
        if not tui_sid:
            raise RuntimeError("gateway session.create returned no session_id")
        self.gateway.sessions[studio_sid] = tui_sid
        self.gateway.tui_sessions[tui_sid] = studio_sid
        self.gateway.session_profiles[tui_sid] = req.get("profile")
        self.gateway.save_state()
        return tui_sid

    async def _action_chat(self, req):
        studio_sid = str(req.get("session_id") or "").strip() or uuid.uuid4().hex
        message = req.get("message", req.get("input", ""))
        tui_sid = await self._ensure_tui_session(studio_sid, req)
        run = self.gateway.new_run(studio_sid, tui_sid)  # 先建 run 再提交（事件竞态修复）
        result = await self.gateway.call("prompt.submit", {"session_id": tui_sid, "text": message})
        # M2：steer/redirect → 新消息并入当前 turn，旧 run 结束（避免悬挂）
        submit_status = (result or {}).get("status") if isinstance(result, dict) else None
        if submit_status in ("steered", "redirected"):
            self.gateway._finish_superseded(tui_sid, run.run_id)
        if req.get("wait"):
            timeout = float(req.get("timeout", 0) or 0)
            deadline = time.time() + timeout if timeout > 0 else None
            while run.status == "running":
                if deadline is not None and time.time() >= deadline:
                    break
                await asyncio.sleep(0.05)
            return run.result_view()
        return {"run_id": run.run_id, "session_id": studio_sid, "status": run.status}

    async def _action_get_output(self, req):
        run = self.gateway.runs.get(str(req.get("run_id") or ""))
        if run is None:
            raise KeyError(f"unknown run: {req.get('run_id')}")
        return run.output_view(int(req.get("cursor") or 0), int(req.get("event_cursor") or 0))

    async def _action_get_result(self, req):
        run = self.gateway.runs.get(str(req.get("run_id") or ""))
        if run is None:
            raise KeyError(f"unknown run: {req.get('run_id')}")
        return run.result_view()

    # ── 控制：interrupt / steer / boundary ──

    def _tui_sid_for(self, req) -> str | None:
        return self.gateway.sessions.get(str(req.get("session_id") or ""))

    async def _action_interrupt(self, req):
        tui_sid = self._tui_sid_for(req)
        if tui_sid:
            await self.gateway.call("session.interrupt", {"session_id": tui_sid})
        return {}

    async def _action_request_boundary_interrupt(self, req):
        # tui_gateway 无 boundary 概念，退化为普通 interrupt
        return await self._action_interrupt(req)

    async def _action_steer(self, req):
        text = str(req.get("text") or req.get("message") or "").strip()
        if not text:
            raise ValueError("text is required")
        tui_sid = self._tui_sid_for(req)
        if tui_sid:
            await self.gateway.call("session.steer", {"session_id": tui_sid, "text": text})
        return {}

    # ── 交互：approval / clarify / compression ──

    async def _action_approval_respond(self, req):
        approval_id = str(req.get("approval_id") or "").strip()
        if not approval_id:
            raise ValueError("approval_id is required")
        choice = str(req.get("choice") or "deny")
        srq_id = self.gateway.approval_map.pop(approval_id, None)
        if srq_id is None:
            raise KeyError(f"unknown approval: {approval_id}")
        entry = self.gateway.pending_srq.pop(srq_id, None)
        await self.gateway.reply_srq(srq_id, {"choice": choice, "all": choice == "always"})
        run = self.gateway.runs.get((entry or {}).get("run_id") or "")
        if run:
            run.append_event({
                "event": "approval.resolved",
                "run_id": (entry or {}).get("run_id") or "",
                "approval_id": approval_id,
                "choice": choice,
            })
        return {"approval_id": approval_id, "resolved": True}

    async def _action_clarify_respond(self, req):
        clarify_id = str(req.get("clarify_id") or "").strip()
        if not clarify_id:
            raise ValueError("clarify_id is required")
        response = str(req.get("response") or "").strip()
        srq_id = self.gateway.clarify_map.pop(clarify_id, None)
        if srq_id is None:
            raise KeyError(f"unknown clarify: {clarify_id}")
        self.gateway.pending_srq.pop(srq_id, None)
        await self.gateway.reply_srq(srq_id, {"answer": response})
        return {"clarify_id": clarify_id, "resolved": True}

    async def _action_compression_respond(self, req):
        # tui_gateway 无 compression 请求；优雅降级
        return {}

    # ── 会话：history / title / status / list / destroy ──

    async def _action_get_history(self, req):
        studio_sid = str(req.get("session_id") or "")
        tui_sid = self.gateway.sessions.get(studio_sid)
        if not tui_sid:
            raise KeyError(f"unknown session: {studio_sid}")
        result = await self.gateway.call("session.history", {"session_id": tui_sid})
        return {"session_id": studio_sid, "history": result.get("messages", [])}

    async def _action_get_session_title(self, req):
        studio_sid = str(req.get("session_id") or "")
        if not studio_sid:
            raise ValueError("session_id is required")
        tui_sid = self.gateway.sessions.get(studio_sid)
        title = ""
        if tui_sid:
            title = self.gateway.session_titles.get(tui_sid, "")
            if not title:
                result = await self.gateway.call("session.title", {"session_id": tui_sid})
                title = str(result.get("title") or "")
                if title:
                    self.gateway.session_titles[tui_sid] = title
        return {"session_id": studio_sid, "title": title}

    async def _action_status(self, req):
        studio_sid = str(req.get("session_id") or "")
        tui_sid = self.gateway.sessions.get(studio_sid)
        if not tui_sid:
            return {"session_id": studio_sid, "exists": False, "running": False, "message_count": 0}
        run = self.gateway.latest_run_for_studio(studio_sid)
        return {
            "session_id": studio_sid,
            "exists": True,
            "running": bool(run and run.status == "running"),
            "current_run_id": run.run_id if run else None,
            "message_count": self.gateway.session_turns.get(tui_sid, 0),
        }

    async def _action_status_if_loaded(self, req):
        return await self._action_status(req)

    async def _action_list(self, req):
        sessions = []
        for studio_sid in self.gateway.sessions:
            run = self.gateway.latest_run_for_studio(studio_sid)
            sessions.append({
                "session_id": studio_sid,
                "running": bool(run and run.status == "running"),
                "current_run_id": run.run_id if run else None,
                "boundary_interrupt": {
                    "supported": False,
                    "phase": None,
                    "pending_run_id": None,
                    "reached_run_id": None,
                    "error": None,
                },
            })
        return {"sessions": sessions}

    async def _action_destroy(self, req):
        studio_sid = str(req.get("session_id") or "")
        tui_sid = self.gateway.sessions.get(studio_sid)
        if tui_sid:
            try:
                await self.gateway.call("session.close", {"session_id": tui_sid})
            except Exception:
                pass
            self._forget_session(studio_sid, tui_sid)
        return {}

    def _forget_session(self, studio_sid: str, tui_sid: str):
        self.gateway.sessions.pop(studio_sid, None)
        self.gateway.tui_sessions.pop(tui_sid, None)
        self.gateway.session_titles.pop(tui_sid, None)
        self.gateway.session_profiles.pop(tui_sid, None)
        self.gateway.session_turns.pop(tui_sid, None)
        self.gateway.save_state()

    async def _action_destroy_all(self, req):
        for studio_sid, tui_sid in list(self.gateway.sessions.items()):
            try:
                await self.gateway.call("session.close", {"session_id": tui_sid})
            except Exception:
                pass
            self._forget_session(studio_sid, tui_sid)
        return {}

    async def _action_destroy_profile(self, req):
        """M4：只关闭指定 profile 的会话（未指定 profile 时等同 destroy_all）。"""
        profile = req.get("profile")
        if not profile:
            return await self._action_destroy_all(req)
        for studio_sid, tui_sid in list(self.gateway.sessions.items()):
            if self.gateway.session_profiles.get(tui_sid) == profile:
                try:
                    await self.gateway.call("session.close", {"session_id": tui_sid})
                except Exception:
                    pass
                self._forget_session(studio_sid, tui_sid)
        return {}

    async def _action_shutdown(self, req):
        # 官方会停进程；本 bridge 是常驻共享服务，保持存活（有意偏离，见 PLAN.md）
        return {"status": "shutting_down", "cleanup": {}}

    # ── 优雅降级（tui 无对应物）──

    async def _action_background_poll(self, req):
        return {}

    async def _action_background_notification_complete(self, req):
        return {}

    async def _action_background_notification_release(self, req):
        return {}

    async def _action_goal_evaluate(self, req):
        return {}

    async def _action_goal_pause(self, req):
        return {}

    # ── 管理功能基础版（M5 完整对接）──

    async def _action_context_estimate(self, req):
        """M5：session.context_breakdown 近似映射（字段联调修正，见 PLAN R1/R5）。"""
        session_id = str(req.get("session_id") or "").strip() or uuid.uuid4().hex
        tui_sid = self.gateway.sessions.get(session_id)
        if not tui_sid:
            # 官方对未知 session 也可估算（基于传入 messages）；此处返回零值近似
            return {"fixed_context_tokens": 0, "system_prompt_tokens": 0, "tool_tokens": 0,
                    "system_prompt_chars": 0, "tool_count": 0, "tool_names": [],
                    "profile": req.get("profile") or "default",
                    "model": req.get("model") or "", "provider": req.get("provider") or ""}
        try:
            result = await self.gateway.call("session.context_breakdown", {"session_id": tui_sid})
        except Exception:
            result = {}
        r = result or {}
        fixed = (r.get("total") or r.get("total_tokens")
                or (r.get("system_tokens") or 0) + (r.get("tool_tokens") or 0)
                + (r.get("history_tokens") or 0) or 0)
        return {
            "fixed_context_tokens": int(fixed or 0),
            "system_prompt_tokens": int(r.get("system_tokens") or 0),
            "tool_tokens": int(r.get("tool_tokens") or 0),
            "system_prompt_chars": int(r.get("system_chars") or 0),
            "tool_count": int(r.get("tool_count") or 0),
            "tool_names": list(r.get("tool_names") or []),
            "profile": req.get("profile") or "default",
            "model": req.get("model") or "",
            "provider": req.get("provider") or "",
        }

    async def _action_provider_credentials(self, req):
        """M5：降级实现——tui_gateway 无直接对应（model.save_key 是写入非查询）。
        返回未配置状态，studio 模型选择器按无凭据处理。"""
        return {"provider": str(req.get("provider") or ""), "configured": False,
                "api_key": "", "base_url": ""}

    async def _action_command(self, req):
        session_id = str(req.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("session_id is required")
        tui_sid = self.gateway.sessions.get(session_id)
        if not tui_sid:
            raise KeyError(f"unknown session: {session_id}")
        result = await self.gateway.call(
            "command.dispatch", {"session_id": tui_sid, "command": str(req.get("command") or "")})
        return result if isinstance(result, dict) else {}

    async def _action_skills_reload(self, req):
        await self.gateway.call("skills.reload", {})
        return {}

    async def _action_switch_session_model(self, req):
        session_id = str(req.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("session_id is required")
        model = str(req.get("model") or "").strip()
        if not model:
            raise ValueError("model is required")
        tui_sid = self.gateway.sessions.get(session_id)
        if not tui_sid:
            raise KeyError(f"unknown session: {session_id}")
        provider = str(req.get("provider") or "").strip()
        target = f"{provider}:{model}" if provider else model
        result = await self.gateway.call(
            "command.dispatch", {"session_id": tui_sid, "command": f"/model {target}"})
        return result if isinstance(result, dict) else {}


async def _gc_loop(gateway: GatewayClient, interval: float = 60.0):
    """M2.5：周期清理已完成且超期的 run；顺带持久化状态（标题等低频变更）。"""
    while True:
        await asyncio.sleep(interval)
        try:
            gateway.gc_runs()
            gateway.save_state()
        except Exception:
            log.exception("run GC 异常")


async def main():
    parser = argparse.ArgumentParser(description="hermes-studio ↔ hermes gateway bridge")
    parser.add_argument("--listen", default="0.0.0.0:18765", help="bridge 监听地址 host:port")
    parser.add_argument("--gateway", required=True, help="gateway WS 地址 ws://host:port/api/ws")
    parser.add_argument("--token", default="", help="gateway WS token (?token=)")
    parser.add_argument("--state-file", default="", help="会话映射持久化路径（默认不持久化）")
    parser.add_argument("--debug", action="store_true", help="debug 日志")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    host, port = args.listen.rsplit(":", 1)
    gateway = GatewayClient(args.gateway, args.token)
    if args.state_file:
        gateway.state_file = args.state_file
        gateway.load_state()
    await gateway.connect()
    if args.state_file:
        await gateway.reconcile()  # M4：启动对账（丢弃 gateway 侧已消失的会话映射）

    bridge = BridgeServer(gateway)
    server = await asyncio.start_server(bridge.handle, host, int(port))
    gc_task = asyncio.create_task(_gc_loop(gateway))
    log.info("bridge 监听 tcp://%s:%s（一连接一请求，官方协议）", host, port)
    log.info("gateway: %s", args.gateway)

    try:
        async with server:
            await server.serve_forever()
    finally:
        gc_task.cancel()
        await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())