# hermes-studio ↔ hermes gateway 自研 Bridge — 实现规划

> 版本：v1.0（2026-09-20）
> 状态：M0 已完成（核心链路验证通过），M1-M7 待实施
> 依据：官方 bridge 参考实现（hermes-studio v0.7.20 npm 包 `dist/server/agent-bridge/python/`）+ hermes-agent tui_gateway OpenRPC 契约（218 方法 / 621 schema，`apps/shared/src/gateway-contract.openrpc.json`）

---

## 1. 背景与目标

### 1.1 问题

hermes-studio 的架构是「宿主」：它通过自带的 `hermes_bridge.py` **import hermes 的 Python 代码**（`from run_agent import AIAgent`）在进程内运行 agent。因此官方 Docker 镜像必须内置完整 hermes 运行时（Python + venv），且无法连接一个**已经独立运行的 hermes 实例**。

### 1.2 目标

实现一个独立 bridge 服务，使：

```
hermes-studio (干净容器，无 hermes)
    │  bridge 协议（TCP JSON Lines，37 action）
    ▼
自研 bridge（本项目）
    │  tui_gateway JSON-RPC（WebSocket /api/ws）——与 Hermes Desktop 完全相同的连接方式
    ▼
hermes serve / hermes gateway（用户已运行的实例，不做任何修改）
```

**验收定义**：hermes-studio 干净镜像 + 本 bridge + 用户已有 gateway，三者组合下：
- 聊天（流式输出、工具调用展示、审批/澄清交互）完整可用
- 会话列表/历史/标题可用
- hermes-studio 启动无报错，`agent bridge started`

### 1.3 非目标

- 不追求 100% 功能对等（goal_*、background delegation 等 bridge 特有功能在 tui_gateway 无对应物，做优雅降级）
- 不修改 hermes-studio 或 hermes-agent 的任何代码
- 不支持 hermes-studio 的桌面端专属功能（MCU 固件、LAN peer 等，它们不走 bridge）

---

## 2. 已验证的协议事实（调研结论）

### 2.1 hermes-studio bridge 协议（左侧，权威来源：官方 Python 实现）

**传输与连接模型**：
- TCP，JSON Lines（每行一个 JSON 对象）
- **一连接一请求**：accept → 读一行 → 处理 → 写响应 → close（官方 `serve_forever` 循环）
- 响应统一包装：
  - 成功：`{"ok": true, **action_result}`
  - 异常：`{"ok": false, "error": "<str>", "error_type": "<类名>"}`
- hermes-studio 客户端校验 `resp.ok`，false 即抛错

**run 数据模型**（`bridge_pool.py`）：
- `record.deltas`：**字符串分块列表**（不是拼接缓冲区）
- `record.events`：事件对象列表
- `record.status`：`running` → `completed` / `failed`（等）
- `record.result`：完成后的结果对象

**核心 action 精确响应格式**（已逐字段核对官方源码）：

```
ping → {pong: true, time, pid, agent_root, profile, hermes_home,
        session_count, running_session_count}

chat → {run_id, session_id, status: "running"}
       （wait:true 时改为轮询阻塞，返回 get_result 形状）

get_output → {run_id, session_id, status,
              delta: "".join(deltas[cursor:]),   ← 增量 = 分块切片拼接
              cursor: len(deltas),               ← 游标 = 分块数（非字符数！）
              output: "".join(deltas),           ← 全量
              done: status != "running",
              result: 完成时的结果 | null,
              error, events: events[event_cursor:],
              event_cursor: len(events)}

get_result → {run_id, session_id, status, started_at, ended_at,
              output, deltas, events, result, error}

get_history → {session_id, history: [...]}
              （未知 session 抛 KeyError → ok:false）

get_session_title → {session_id, title: str}

status → {session_id, exists, running, current_run_id, ...}
         （未知 session 返回 exists:false，不抛错）

list → {sessions: [{session_id, running, current_run_id,
        boundary_interrupt: {supported, phase, pending_run_id,
                             reached_run_id, error}}, ...]}

clarify_respond → {clarify_id, resolved: true}

shutdown → {status: "shutting_down", cleanup: {...}}
```

**事件对象格式**（追加进 `record.events`，前端渲染依赖）：

```
{"event": "status", "kind", "text"}
{"event": "thinking.delta", "text": ""}          ← 官方恒为空串（防 spinner 污染）
{"event": "reasoning.delta", "text"}
{"event": "reasoning.available", "text"}
{"event": "tool.started", "tool_call_id", "tool_name", "args"}
{"event": "tool.completed", "tool_call_id", "tool_name", "args", "result", ...}
{"event": "step", "step_info"}
{"event": "approval.requested", "run_id", "approval_id", "command",
 "description", "choices": ["once","session","always","deny"],
 "allow_permanent", "timeout_ms"}
{"event": "approval.resolved", "run_id", "approval_id", "choice"}
{"event": "clarify.requested", "clarify_id", "question", "choices", "timeout_ms"}
{"event": "moa.reference" / "moa.aggregating", ...}
{"event": "subagent.*", ...}
```

**审批/澄清机制**：agent 回调阻塞在 `queue.get(timeout)`；`approval_respond`/`clarify_respond` 往队列放答案。超时默认 deny / "[user did not respond]"。

### 2.2 tui_gateway 协议（右侧，权威来源：OpenRPC 契约 + Desktop 客户端源码）

**传输与握手**（与 Desktop `JsonRpcGatewayClient` 逐项一致，已核对）：
- WebSocket `ws://host:port/api/ws`，认证 query 参数：`?token=`（insecure/显式）| `?ticket=`（单次 30s TTL）| `?internal=`
- 换行分隔 JSON-RPC 2.0，双向
- 连接即收 `gateway.ready` 事件（`{skin, change_events, replay_epoch, heartbeat}`）
- 客户端须回 `client.capabilities {server_requests: true}`，否则所有 server→client 请求被立即拒绝
- 心跳：`heartbeat` 声明支持时周期发 `gateway.ping`
- 断线回放：`session.events.since {session_id, last_seen}` → `{events, latest_seq, truncated, epoch, open_requests}`

**核心方法**：

```
session.create {profile?, cwd?, messages?, title?, model?, provider?,
                reasoning_effort?, close_on_disconnect, hidden}
  → {session_id, stored_session_id, message_count, messages, info}

prompt.submit {session_id, text, profile?, [truncate_* 参数]}
  → {status: "streaming"|"queued"|"steered"|"redirected"|null, ...}
  ⚠️ 不返回 run_id —— run 概念是 bridge 侧自造的

session.interrupt {session_id}
session.steer {session_id, text}
session.list {} → {sessions: [...]}
session.history {session_id} → {count, messages: [TranscriptMessage]}
  TranscriptMessage: {role, text, timestamp, row_id, display_kind,
                      display_metadata, name, context, args, reasoning}
session.resume {session_id} → 含 inflight / open_requests / pending_approval
session.title / session.status / session.usage / session.close / session.delete
session.compress / session.context_breakdown / session.most_recent
session.events.since {session_id, last_seen}   ← 回放
command.dispatch（slash 命令）
model.options / model.save_key / model.disconnect
mcp.servers.list / add / remove / test / status / set_api_key / oauth.*
mcp.catalog
skills.reload / skills.manage
reload.mcp / reload.env
approval.respond / clarify.lock（客户端直调变体）
ping
```

**事件流**（`method:"event"`，`params.type` 区分）：

```
message.delta    → StreamDeltaPayload {text, rendered?, verbose?}
message.interim  → {text, already_streamed}
message.complete → {text, usage, status, reasoning, warning, error,
                    recoverable, failure_reason, billing, ...}
tool.start       → {tool_id, name, context, args, args_text, preview}
tool.generating  → {name}
tool.complete    → {tool_id, name, args, duration_s, result, summary,
                    result_text, inline_diff, todos, revision}
session.title    → {session_id, title}
request.cancel   → {id, method, reason}
```

**server→client 请求**（JSON-RPC request，`srq-<n>` id，须应答同 id 的 result）：

```
approval → 参数 {session_id, request_id, command, description, choices,
                 allow_permanent, allow_session, smart_denied, tool_name,
                 gateway_session_id}
           应答 {choice, all}
clarify   → 参数 {session_id, question, choices, multi_select, questions, answers}
           应答 {answer, answers}（单选 answer，批量 answers，取消 {}）
sudo      → {session_id, command} → {value}
secret    → {session_id, env_var, prompt, metadata} → {value}
connection → {settled_by, targets}
terminal.read / window.read / preview.act → {value}
不实现的方法须回 JSON-RPC error -32601（快速失败，否则 agent 等到超时）
```

---

## 3. 完整映射表（37 action → tui_gateway）

| # | bridge action | tui_gateway 对应 | 可行性 | 备注 |
|---|---|---|---|---|
| 1 | ping | 本地状态 + ping | ✅ | |
| 2 | chat | session.create? + prompt.submit | ✅ | M1 修正响应格式 |
| 3 | get_output | 本地事件缓冲 | ✅ | M1 补齐字段/游标语义 |
| 4 | get_result | 本地 | ✅ | |
| 5 | interrupt | session.interrupt | ✅ | |
| 6 | request_boundary_interrupt | session.interrupt | ⚠️ 近似 | tui 无 boundary 概念，直接 interrupt |
| 7 | steer | session.steer | ✅ | |
| 8 | approval_respond | 应答 srq approval | ✅ | M3：id 映射 + choices 语义转换 |
| 9 | clarify_respond | 应答 srq clarify | ✅ | M3 |
| 10 | compression_respond | — | ➖ 空实现 | tui 无 compression 请求；返回 ok |
| 11 | get_history | session.history | ✅ | M4：TranscriptMessage → studio history 格式转换 |
| 12 | get_session_title | session.title | ✅ | M4 |
| 13 | command | command.dispatch | ✅ | M5 |
| 14 | skills_reload | skills.reload | ✅ | M5 |
| 15 | switch_session_model | command.dispatch("/model …") | ⚠️ | M5；或 model.options 辅助 |
| 16 | goal_evaluate | — | ➖ 空实现 | bridge 特有 |
| 17 | goal_pause | — | ➖ 空实现 | bridge 特有 |
| 18 | status | session.status | ✅ | M4 |
| 19 | background_poll | — | ➖ ok | M1 |
| 20 | background_notification_complete | — | ➖ ok | |
| 21 | background_notification_release | — | ➖ ok | |
| 22 | destroy | session.close | ✅ | M4 |
| 23 | destroy_all | 逐 session.close | ✅ | M4 |
| 24 | destroy_profile | close 该 profile 全部会话 | ⚠️ | M4 |
| 25 | list | session.list | ✅ | M4：格式转换 |
| 26 | shutdown | 断开 WS（可选重连） | ✅ | |
| 27 | mcp_list | mcp.servers.list | ✅ | M5 |
| 28 | mcp_server_add | mcp.servers.add | ✅ | M5 |
| 29 | mcp_server_update | mcp.servers.add（覆盖语义确认） | ⚠️ | M5 联调确认 |
| 30 | mcp_server_remove | mcp.servers.remove | ✅ | M5 |
| 31 | mcp_server_test | mcp.servers.test | ✅ | M5 |
| 32 | mcp_tools_list | mcp.servers.status / mcp.catalog | ⚠️ | M5 联调确认 |
| 33 | mcp_reload | reload.mcp | ✅ | M5 |
| 34 | context_estimate | session.context_breakdown | ⚠️ 近似 | M5；字段映射联调 |
| 35 | provider_credentials | model.save_key 等 | ⚠️ | M5；可能降级 |
| 36 | status_if_loaded | 本地缓存 | ✅ | M4 |
| 37 | get_session_title（去重） | session.title | ✅ | |

图例：✅ 完整可行 ｜ ⚠️ 近似/需联调确认 ｜ ➖ 优雅降级（返回 ok 空结果，studio 功能不可用但不崩）

**结论：37 个 action 中 26 个完整可行，6 个近似实现，5 个优雅降级。核心聊天闭环（chat/get_output/interrupt/steer/approval/clarify/history）全部可行。**

---

## 4. 分阶段实施计划

### M1 — 协议保真修正（当前原型 → 协议正确）

**目标**：让 bridge 的响应与官方实现**逐字段一致**，消除 mock 测试发现不了的协议偏差。

| 任务 | 说明 | 验收 |
|---|---|---|
| 1.1 chat 响应修正 | 返回 `{run_id, session_id, status:"running"}`（补 session_id/status）；支持 `wait:true` 阻塞语义 | 契约测试通过 |
| 1.2 get_output 补齐 | 补 `output/result/error/status` 字段；`deltas` 改为**分块列表**，cursor=块数；未知 run 返回 `ok:false`（官方抛 KeyError） | 契约测试 |
| 1.3 错误响应统一 | 异常 → `{"ok":false,"error","error_type"}` | 单测 |
| 1.4 连接模型对齐 | 一连接一请求（保持多请求兼容）；每连接处理超时 | 联调 |
| 1.5 事件格式对齐 | 事件对象用官方字段名（`tool_call_id`/`tool_name`/`args`…） | 对照官方 pool 源码 |
| 1.6 契约测试框架 | 用官方响应形状做 golden 测试（从 2.1 节格式表生成） | 全 action 覆盖 |

工作量：~1 天。产出：`bridge.py` v0.2 + 契约测试套件。

### M2 — 事件桥接引擎（流式核心）

**目标**：tui_gateway 事件 → bridge run 缓冲的完整、无损转换。

| 任务 | 说明 |
|---|---|
| 2.1 RunRegistry | run_id ↔ (session_id, turn) 映射；同 session 串行 turn 队列；run 状态机 running→completed/failed |
| 2.2 事件转换器 | `message.delta`→deltas.append(text)；`message.complete`→done+result+usage；`tool.start`→tool.started 事件；`tool.complete`→tool.completed 事件（result 字段透传） |
| 2.3 turn 结束判定 | `message.complete`（status/error 字段）驱动 run 完成；失败路径（error/recoverable）→ status:"failed" |
| 2.4 事件顺序保证 | 同 session 事件按到达序；run 创建先于 prompt.submit（已知竞态修复固化） |
| 2.5 run GC | 完成后保留 N 分钟，超时清理 |

验收：mock gateway 回放真实事件序列（录制），bridge get_output 轮询产出与官方 bridge 相同的 delta/event 流。

### M3 — 交互闭环（审批/澄清/控制）

| 任务 | 说明 |
|---|---|
| 3.1 srq 请求拦截 | 收到 server→client `approval/clarify/sudo/secret` 请求 → 生成 `approval_id/clarify_id`（uuid hex），缓存映射 srq-id ↔ bridge-id |
| 3.2 事件合成 | 同时向对应 run.events 注入官方格式的 `approval.requested` / `clarify.requested` 事件（含 choices/timeout_ms） |
| 3.3 应答回传 | `approval_respond` → JSON-RPC result `{choice, all:false}`；`clarify_respond` → `{answer}`；choices 语义映射（once/session/always/deny ↔ tui choices） |
| 3.4 未实现方法 | `sudo/secret/connection/terminal.read…` → `-32601`（studio 端这些走别的通道，预期低频） |
| 3.5 request.cancel | 清除对应 pending，注入 resolved 事件 |
| 3.6 interrupt/steer/boundary | 转发 + 注入 status 事件 |

验收：mock gateway 发起 approval 请求 → studio 收到 approval.requested 事件 → studio 回 approval_respond → gateway 收到正确 JSON-RPC result。

### M4 — 会话生命周期

| 任务 | 说明 |
|---|---|
| 4.1 会话映射表 | studio session_id ↔ tui session_id；启动时 `session.list` + `session.most_recent` 对账；**持久化到磁盘**（bridge 重启不丢） |
| 4.2 get_history | `session.history` → studio history 格式（TranscriptMessage 字段转换；**联调时抓 studio 实际消费字段**——风险项） |
| 4.3 list/status/title | 格式转换（boundary_interrupt 恒为 supported:false） |
| 4.4 destroy 系列 | session.close / session.delete；destroy_profile 遍历 |
| 4.5 断线恢复 | bridge 重启后 `session.resume` 恢复映射；inflight turn 重新关联 run |

### M5 — 管理功能

| 任务 | 说明 |
|---|---|
| 5.1 command | studio slash 命令 → `command.dispatch`（参数格式联调） |
| 5.2 switch_session_model | → `/model <provider:model>` dispatch |
| 5.3 MCP 六项 | mcp.servers.* 映射；tools_list/update 的真实形状联调确认 |
| 5.4 skills_reload | skills.reload |
| 5.5 context_estimate | session.context_breakdown 近似（fixed_context_tokens 等字段映射） |
| 5.6 provider_credentials | model.save_key / 降级空实现 |

### M6 — 健壮性与认证

| 任务 | 说明 |
|---|---|
| 6.1 WS 重连 | 指数退避；重连后 `client.capabilities` 重发 |
| 6.2 事件回放 | `session.events.since {last_seen}` 补齐断线期间事件（seq 水位跟踪） |
| 6.3 认证 | ①`?token=`（已有）②用户名/密码登录 → cookie → mint ticket 流程 ③OAuth（Nous Portal，`hermes dashboard register`） |
| 6.4 心跳 | gateway.ready.heartbeat → 周期 gateway.ping；对 studio 的 ping 保持低延迟 |
| 6.5 并发 | 多 studio 客户端同时连 bridge（TCP 层天然并发；run/session 状态加锁） |
| 6.6 背压/限流 | get_output 轮询频率、事件缓冲上限、慢客户端丢弃策略 |

### M7 — 部署集成

| 任务 | 说明 |
|---|---|
| 7.1 bridge Dockerfile | python:3.12-slim + websockets；多阶段；<100MB |
| 7.2 compose 模板 | studio + bridge 两服务；`HERMES_AGENT_BRIDGE_ENDPOINT=tcp://bridge:18765`；gateway 地址/认证参数化 |
| 7.3 GitHub Actions | 并入现有 hermes-studio-docker 仓库构建（bridge 独立镜像 tag） |
| 7.4 配置文档 | README：gateway 侧 `hermes serve --host 0.0.0.0` + 认证配置；studio 侧环境变量；bridge 参数 |
| 7.5 版本策略 | 镜像 tag 跟随 studio 版本（bridge 协议随 studio 版本演进，需同版本配对） |

---

## 5. 关键风险与缓解

| # | 风险 | 等级 | 缓解 |
|---|---|---|---|
| R1 | **真实 gateway 事件字段与 OpenRPC 文档偏差**（文档滞后/条件字段） | 高 | M2 起所有联调用真实 `hermes serve` 抓包对照；契约测试以实测为准修订 |
| R2 | **studio history 格式不明**：get_history 返回的 history 元素结构、chat 的 conversation_history 期望格式，官方源码中由 AIAgent 消息直接填充，未见显式 schema | 高 | M4 联调时先抓官方 bridge 的实际响应（跑一次官方镜像对比）；格式转换器独立成模块便于修 |
| R3 | **run 语义鸿沟**：tui prompt.submit 无 run_id；turn 与 session 的对应关系在多 turn/steer/queued 时复杂 | 高 | RunRegistry 显式建模 turn；steered/queued 状态映射到 run status；联调覆盖多 turn 序列 |
| R4 | **approval choices 语义差异**：bridge 侧 once/session/always/deny + allow_permanent；tui 侧 ApprovalResult {choice, all} | 中 | M3 做双向映射表；always→{choice, all:true}；联调验证 |
| R5 | **studio 版本演进**（上游周更，bridge 协议可能变） | 中 | 契约测试锁定当前格式；workflow 构建时跑契约测试；版本配对发布 |
| R6 | gateway 认证模式差异（insecure/token/ticket/oauth） | 中 | M6 分级支持；文档明确每种模式的 gateway 侧配置 |
| R7 | 工具结果大对象（tool.complete 的 result/inline_diff）在 get_output events 中的体积 | 低 | 事件裁剪开关（保留 result_text/summary，丢 inline_diff 可配置） |
| R8 | hermes-studio 启动探测需要 HERMES_BIN 通过 Python import 检测（`import hermes_cli`） | 已解决 | 干净镜像内置 stub（fake hermes_cli 模块，已验证可行）——**需固化为镜像内标准组件** |

---

## 6. 测试策略

**分层**：

1. **单元测试**：协议转换器（事件映射、格式转换、choices 映射）纯函数化
2. **契约测试（golden）**：左侧对 2.1 节格式表逐 action 断言响应形状；右侧用 OpenRPC schema 校验发出的 JSON-RPC 帧
3. **集成测试**：mock gateway（已有）扩展——录制/回放真实事件序列
4. **E2E 联调（真实组件）**：
   - 真实 `hermes serve`（本地或用户的）+ bridge + studio 干净镜像
   - 手动清单：发消息→流式渲染→工具卡→审批弹窗→应答→中断→转向→历史→会话切换→MCP 页
5. **回归**：studio 升级后重跑契约测试 + E2E 清单

**联调环境要求**（用户提供）：可访问的 `hermes serve` 实例（`--host 0.0.0.0`）+ 认证方式（token 或账密）。

---

## 7. 里程碑与交付物

| 里程碑 | 内容 | 预估 | 交付物 |
|---|---|---|---|
| **M0** ✅ | 核心链路验证 | 已完成 | bridge.py v0.1 + mock E2E 9/9 + studio attach 验证 |
| **M1** | 协议保真 | 1 天 | v0.2 + 契约测试套件 |
| **M2** | 事件引擎 | 2 天 | v0.3 + 事件回放测试 |
| **M3** | 交互闭环 | 1.5 天 | v0.4 + approval E2E |
| **M4** | 会话生命周期 | 2 天 | v0.5 + 持久化 |
| **M5** | 管理功能 | 1.5 天 | v0.6 |
| **M6** | 健壮性/认证 | 2 天 | v0.7 |
| **M7** | 部署集成 | 1 天 | Docker 镜像 + compose + Actions + 文档 |
| **联调** | 真实 gateway 全链路（贯穿 M2 起） | 2-3 天（含修偏） | v1.0 |

总计约 **13-15 个工作日**（含联调修偏缓冲）。

**v1.0 验收清单**：
- [ ] studio 干净镜像 + bridge + 真实 gateway：聊天流式完整
- [ ] 工具调用卡片渲染正常
- [ ] 审批/澄清弹窗 + 应答闭环
- [ ] interrupt / steer 生效
- [ ] 会话列表 / 历史 / 标题正确
- [ ] bridge 重启后会话映射恢复
- [ ] WS 断线重连 + 事件回放
- [ ] compose 一键起全套
- [ ] GitHub Actions 自动构建 bridge 镜像

---

## 8. 附录

### A. 权威资料索引

| 资料 | 位置 | 用途 |
|---|---|---|
| 官方 bridge 参考实现 | hermes-studio npm 包 `dist/server/agent-bridge/python/{bridge_server,bridge_pool}.py` | 左侧协议唯一权威（响应格式/事件格式/行为语义） |
| tui_gateway OpenRPC 契约 | hermes-agent `apps/shared/src/gateway-contract.openrpc.json`（218 方法/621 schema） | 右侧协议权威 |
| Desktop 客户端 | hermes-agent `apps/shared/src/{json-rpc-gateway,json-rpc-channel,websocket-url}.ts` | 连接方式参考（已逐项对齐） |
| 程序化集成文档 | hermes-agent `website/docs/developer-guide/programmatic-integration.md` | 协议总览/方法目录 |
| API server 文档 | `website/docs/user-guide/features/api-server.md` | 备选通道（/v1/responses，本方案未采用） |
| 本地副本 | `/tmp/opencode/openrpc.json`、`/tmp/opencode/webui/`（v0.7.20 npm 包） | 离线查阅 |

### B. 当前实现与官方协议的已知偏差（M1 修正清单）

| 偏差 | 现状 | 官方 |
|---|---|---|
| chat 响应 | `{ok, run_id}` | `{ok, run_id, session_id, status}` |
| get_output.cursor | 字符位置 | 分块索引（len(deltas)） |
| get_output 字段 | 缺 output/result/error/status | 全字段 |
| 未知 run 的 get_output | 返回空 done | 抛错 → ok:false |
| 事件字段名 | tool/start 等 | tool.started + tool_call_id/tool_name/args |
| 错误响应 | `{ok:false, error}` | `{ok:false, error, error_type}` |