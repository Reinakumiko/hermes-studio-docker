# hermes-studio bridge → hermes gateway 协议映射

## 架构

```
hermes-studio (容器) ──(bridge协议: TCP JSON Lines)──> 自研 bridge ──(tui_gateway JSON-RPC over WebSocket /api/ws)──> hermes serve (gateway) ──> hermes agent
```

## 左侧：hermes-studio bridge 协议（客户端 → bridge）

- 传输：TCP，JSON Lines（每行一个 JSON 对象）
- 所有响应必须含 `ok: true`（否则 hermes-studio 认为 bridge 不可达）
- 核心 action（37 个，先实现核心子集）：

| action | 请求 | 响应 |
|---|---|---|
| `ping` | `{action:"ping"}` | `{ok:true, pong:true, time, pid, agent_root, profile, hermes_home, session_count, running_session_count}` |
| `chat` | `{action:"chat", session_id, message, conversation_history?, instructions?, profile?, model?, provider?, workspace?}` | `{ok:true, run_id}` |
| `get_output` | `{action:"get_output", run_id, cursor, event_cursor}` | `{ok:true, cursor, event_cursor, delta?, done?, events?}` |
| `interrupt` | `{action:"interrupt", session_id, message?, profile?}` | `{ok:true}` |
| `steer` | `{action:"steer", session_id, text, profile?}` | `{ok:true}` |
| `get_history` | `{action:"get_history", session_id, profile?}` | `{ok:true, history?}` |
| `status` | `{action:"status", session_id, profile?}` | `{ok:true, status?}` |
| `list` | `{action:"list"}` | `{ok:true, sessions?}` |
| `destroy` | `{action:"destroy", session_id, profile?}` | `{ok:true}` |
| `shutdown` | `{action:"shutdown"}` | `{ok:true}` |
| `background_poll` | `{action:"background_poll", routes?}` | `{ok:true}` |
| `approval_respond` | `{action:"approval_respond", approval_id, choice}` | `{ok:true}` |
| `clarify_respond` | `{action:"clarify_respond", clarify_id, response}` | `{ok:true}` |
| `mcp_list` | `{action:"mcp_list", profile?}` | `{ok:true, servers?}` |
| `get_result` | `{action:"get_result", run_id}` | `{ok:true, output?, status?}` |

## 右侧：tui_gateway JSON-RPC 协议（bridge → gateway）

- 传输：WebSocket `/api/ws`（`hermes serve` 提供），newline-delimited JSON-RPC 2.0
- 握手后服务端发 `gateway.ready`
- 客户端需发 `client.capabilities {server_requests: true}` 声明支持 server→client 请求
- 认证：WS upgrade 时 `?token=`（--insecure）或 `?ticket=` / `?internal=`
- 事件：`message.delta`、`message.complete`、`tool.start`、`tool.complete` 等
- server→client 请求：`approval`、`clarify`、`sudo`、`secret` 等（agent 问用户）

### 核心方法

| 方法 | 参数 | 说明 |
|---|---|---|
| `session.create` | `{profile?, cwd?, messages?, title?, model?, provider?}` | 创建会话 |
| `prompt.submit` | `{session_id, text, profile?}` | 提交聊天 |
| `session.interrupt` | `{session_id, profile?}` | 中断 |
| `session.steer` | `{session_id, text, profile?}` | 转向 |
| `session.list` | `{}` | 列出会话 |
| `session.resume` | `{session_id?}` | 恢复会话 |
| `session.info` | `{session_id}` | 会话信息 |
| `session.history` | `{session_id}` | 会话历史 |
| `ping` | `{}` | 心跳 |
| `client.capabilities` | `{server_requests: true}` | 声明能力 |

### 事件流

| 事件 | 载荷 | 说明 |
|---|---|---|
| `message.delta` | `{session_id, delta, ...}` | 增量文本 |
| `message.complete` | `{session_id, ...}` | 消息完成 |
| `tool.start` / `tool.complete` | `{session_id, tool_name, ...}` | 工具调用 |
| `request.cancel` | `{id, method, reason}` | 请求取消 |

## 映射逻辑（bridge 核心）

### chat → prompt.submit

1. bridge 收到 `chat {session_id, message, ...}`
2. 若无 session → 先 `session.create`（或复用 session_id 对应的 tui session）
3. 发 `prompt.submit {session_id, text: message}`
4. 返回 `{ok:true, run_id: <tui run/session id>}` 给 hermes-studio

### get_output → 事件流

1. bridge 收到 `get_output {run_id, cursor, event_cursor}`
2. 从该 run 的已缓存事件流中，返回 cursor 之后的增量：
   `{ok:true, cursor: <新cursor>, event_cursor: <新>, delta: <增量文本>, done: <是否完成>}`
3. 事件来源：`message.delta`（文本增量）、`message.complete`（完成标记）

### interrupt → session.interrupt

1. bridge 收到 `interrupt {session_id}`
2. 发 `session.interrupt {session_id}`
3. 返回 `{ok:true}`

### approval/clarify（server→client 请求）

1. gateway 发 `approval`/`clarify` JSON-RPC 请求给 bridge
2. bridge 缓存为 pending 状态
3. hermes-studio 调 `approval_respond`/`clarify_respond` 时，bridge 回 JSON-RPC 响应

## 认证配置（gateway 侧）

`hermes serve` 绑定非 loopback 时启用 auth gate：
- 用户名/密码（LAN/VPN 用）
- 或 OAuth（Nous Portal）
- `--insecure` 绕过（仅无 public_url 配置时）

bridge 连接 WS 时：
- `--insecure` 模式：`ws://host:port/api/ws?token=<token>`
- 用户名/密码：需先登录拿 session，再换 ticket（复杂，先支持 insecure/token）
