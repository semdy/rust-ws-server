# rust-ws-server

生产取向的 Rust WebSocket 服务端示例，基于 Tokio + Axum WebSocket。

## 特性

- 连接数上限控制，避免文件描述符和内存被打爆
- 每连接有界写队列，慢消费者不会拖垮全局
- 多主题广播，避免所有消息走单一全局队列
- DashMap 管理在线客户端和主题，降低热点锁竞争
- 每连接消息速率限制，异常客户端会被主动关闭
- Ping/Pong 与空闲超时，主动清理僵尸连接
- `/healthz`、`/readyz`、`/metrics` 运维端点
- MessagePack 二进制出站协议，内部使用 `bytes::Bytes` 共享消息 buffer
- 兼容 JSON 文本入站，也支持 MessagePack 二进制入站
- 结构化日志、优雅退出
- 集成测试覆盖发布、广播、私聊、限流、MessagePack 入站路径

## 运行

```bash
cargo run --release
```

默认监听 `0.0.0.0:8080`，WebSocket 地址为：

```text
ws://127.0.0.1:8080/ws?topic=public&client_id=alice
```

## 测试

```bash
cargo test                          # 全部测试（单元 + 集成）
cargo test --lib                    # 仅单元测试（src/ 内的 #[cfg(test)] 模块）
cargo test --test websocket         # 仅集成测试（tests/websocket.rs）
cargo test --test websocket api_    # 按名称过滤
cargo test auth::tests              # 指定模块
cargo test api_                     # 按名称过滤，跑所有 API 相关测试
```

测试覆盖：鉴权、多租户隔离、IP/租户限流、广播、私聊、MessagePack 入站、HTTP API。

## 配置

所有配置都支持环境变量：

```bash
WS_BIND_ADDR=0.0.0.0:8080 \
WS_MAX_CONNECTIONS=10000 \
WS_CLIENT_QUEUE_CAPACITY=256 \
WS_TOPIC_CHANNEL_CAPACITY=1024 \
WS_MAX_MESSAGES_PER_SECOND=100 \
WS_MESSAGE_BURST=200 \
WS_IDLE_TIMEOUT=60s \
WS_HEARTBEAT_INTERVAL=20s \
WS_JWT_SECRET=your-hmac-secret \
WS_IP_MAX_CONCURRENT=200 \
WS_IP_CONNECTION_RATE=20 \
WS_IP_RATE_BURST=40 \
WS_TRUST_PROXY_HEADERS=true \
WS_TENANT_MAX_CONNECTIONS=500 \
WS_TENANT_MAX_MESSAGES_PER_SECOND=200 \
WS_TENANT_MESSAGE_BURST=400 \
cargo run --release
```

### `.env` 自动加载

启动时（`Config::parse` 之前）会尝试加载项目根目录的 `.env` 文件，存在则自动注入环境变量，不存在则跳过。便于本地开发，生产环境仍走真实环境变量。

```bash
# .env （已 gitignore，不会入库）
WS_JWT_SECRET=your-hmac-secret
WS_TENANT_MAX_CONNECTIONS=500
WS_TENANT_MAX_MESSAGES_PER_SECOND=200
```

```bash
cargo run --release   # 自动读取上面的 .env
```

也可继续用命令行前缀或 `export` 覆盖，优先级与 std::env 一致。

### 鉴权（JWT）

配置 `WS_JWT_SECRET`（HS256）或 `WS_JWT_PUBLIC_KEY`（RS256/EdDSA PEM）即启用 JWT 鉴权。
都不配则鉴权关闭（dev 模式，启动时会有 warn 日志）。

握手时通过 query 参数 `?token=<jwt>` 传递 token。JWT payload 约定：

| 字段 | 必需 | 说明 |
|------|------|------|
| `sub` | 是 | 客户端身份，作为可信 `client_id`（覆盖 query 里的 `client_id`） |
| `tenant_id` | 否 | 租户 ID。缺失时归入 `default` 租户 |
| `exp` | 是 | 过期时间，强制校验 |
| `iss` | 否 | 可选签发方；配置 `WS_JWT_ISSUER` 后才校验 |

#### Token 生成

**方式一：`mint-token` 示例（推荐，与项目同一套库）**

仓库提供 `examples/mint-token.rs`，读 `WS_JWT_SECRET` 签发 HS256 token：

```bash
WS_JWT_SECRET=your-hmac-secret \
CLIENT_ID=alice \
TENANT_ID=t1 \
TTL_SECS=3600 \
cargo run --example mint-token
```

输出即为 token，拼到 URL 里：

```text
ws://127.0.0.1:8080/ws?topic=public&token=eyJhbGciOiJIUzI1NiIs...
```

环境变量说明：

| 变量 | 必需 | 说明 |
|------|------|------|
| `WS_JWT_SECRET` | 是 | HMAC 密钥，必须与服务端一致 |
| `CLIENT_ID` | 否 | `sub` 字段，缺省 `alice` |
| `TENANT_ID` | 否 | `tenant_id` 字段，缺省归入 `default` |
| `TTL_SECS` | 否 | 有效期秒数，缺省 3600 |
| `ISS` | 否 | `iss` 字段，仅服务端配了 `WS_JWT_ISSUER` 时需要 |

**方式二：`jwt-cli` 命令行（不开 Rust）**

```bash
# 安装：cargo install jwt-cli
jwt encode --secret "your-hmac-secret" --alg HS256 \
  --exp "$(date -v+1H +%s)" \
  '{"sub":"alice","tenant_id":"t1"}'
```

关键字段

| 字段 | 必需 | 说明 |
|------|------|------|
| `sub` | 是 | 成为 `client_id` |
| `tenant_id` | 否 | 缺省归 `default` |
| `exp` | 是 | Unix 秒，过期失效 |
| `iss` | 否 | 配了 `WS_JWT_ISSUER` 才校验 |

两种方式产出的 token 等价，服务端只校验签名和 `exp`。

### 多租户

`tenant_id` 的来源取决于鉴权模式：

| 模式 | tenant_id 来源 | 说明 |
|------|----------------|------|
| JWT 启用 | JWT 的 `tenant_id` claim | URL 里的 `?tenant_id=` 被静默忽略（JWT claim 是权威来源） |
| JWT 关闭 | URL 的 `?tenant_id=`，缺省 `default` | 便于本地开发/测试多租户语义；生产环境必须开 JWT |

无论哪种来源，启用后即生效：

- 同名主题在不同租户间互不可见（`t1:room-a` 与 `t2:room-a` 物理隔离）
- `direct` 私聊限定同租户，跨租户发送按 `client_not_found` 处理（不泄露目标存在性）
- 同一 `client_id` 可在不同租户中并存
- 配置 `WS_TENANT_MAX_CONNECTIONS` 后，每个租户有独立的并发连接上限，防止某个吵闹租户打满全局连接池饿死其他租户；不同租户的配额互不影响
- 配置 `WS_TENANT_MAX_MESSAGES_PER_SECOND` / `WS_TENANT_MESSAGE_BURST` 后，每个租户的入站消息聚合计数令牌桶，超限消息被丢弃并返回 `tenant_rate_limited` error（**不关闭连接**，因为单连接不应为租户级聚合行为背锅）；不同租户的桶独立

### IP 限流

| 环境变量 | 说明 |
|---------|------|
| `WS_IP_MAX_CONCURRENT` | 单 IP 最大并发连接数，不配则不限 |
| `WS_IP_CONNECTION_RATE` | 单 IP 每秒新建连接数上限，不配则不限 |
| `WS_IP_RATE_BURST` | 令牌桶突发容量，缺省取 `WS_IP_CONNECTION_RATE` |
| `WS_TRUST_PROXY_HEADERS` | 是否信任 `X-Forwarded-For` / `X-Real-IP`。**仅在可信反代后开启**，否则客户端可伪造 IP |

## 客户端消息

开启 JWT 鉴权后，握手 URL 必须带 `?token=<jwt>`，例如：

```text
ws://127.0.0.1:8080/ws?topic=public&token=eyJhbGciOiJIUzI1NiIs...
```

`client_id` 不再由 query 提供，而是从 JWT 的 `sub` 字段派生。鉴权关闭时（未配置 `WS_JWT_SECRET`），仍可通过 `?client_id=` 指定。

客户端入站兼容两种格式：

- 浏览器/调试友好：发送 JSON 文本 frame
- 高性能路径：发送 MessagePack 二进制 frame

服务端出站统一返回 MessagePack 二进制 frame。Web 示例页面已经内置当前协议所需的轻量 MessagePack 解码器。

#### MessagePack 入站示例

浏览器（配合 `msgpackr`、`@msgpack/msgpack`、`msgpack-lite` 等库）：

```js
import { pack, unpack } from 'msgpackr';
const buf = pack({
  kind: "publish",
  request_id: "r1",
  payload: { text: "hello msgpack" }
});
// const obj = unpack(buf);
socket.send(new Uint8Array(buf));   // 发送 binary frame
```

Rust 客户端发送 MessagePack binary frame 的形态：

```rust
let bytes = rmp_serde::to_vec_named(&serde_json::json!({
    "kind": "publish",
    "request_id": "r1",
    "payload": { "text": "hello msgpack" }
}))?;
ws.send(Message::Binary(bytes.into())).await?;
```

两种入站格式可混用，服务端按 frame 类型分派解析。

发布消息到当前主题：

```json
{"kind":"publish","request_id":"1","payload":{"text":"hello"}}
```

发布到指定主题：

```json
{"kind":"publish","topic":"room-a","payload":{"text":"hello room-a"}}
```

应用层 ping：

```json
{"kind":"ping","request_id":"2","payload":null}
```

发送点对点私聊消息，`to` 是目标连接的 `client_id`：

```json
{"kind":"direct","to":"bob","request_id":"3","payload":{"text":"hello bob"}}
```

服务端消息示例：

```json
{"kind":"message","topic":"public","from":"alice","request_id":"1","payload":{"text":"hello group"}}
```

```json
{"kind":"direct_message","from":"alice","to":"bob","request_id":"3","payload":{"text":"hello bob"}}
```

Rust 客户端发送 MessagePack binary frame 的形态：

## Web 客户端示例

已提供一个浏览器调用示例：

```text
examples/web-client/index.html
```

使用步骤：

1. 启动服务端：

```bash
cargo run --release
```

2. 用浏览器打开 `examples/web-client/index.html`。

3. 点击“连接”，默认会连接：

```text
ws://127.0.0.1:8080/ws?topic=public&client_id=web-demo&tenant_id=default
```

页面里支持群聊和点对点私聊。测试私聊时可以打开两个浏览器标签页：

- 标签页 A：`client_id=alice`
- 标签页 B：`client_id=bob`

两个标签页连接同一个服务端后，A 选择“私聊”并把目标设为 `bob`，只有 B 会收到 `direct_message`。

页面里可以发送群聊：

```js
socket.send(JSON.stringify({
  kind: "publish",
  request_id: "web-1",
  payload: { text: "hello from browser" }
}));
```

也可以发送私聊：

```js
socket.send(JSON.stringify({
  kind: "direct",
  to: "bob",
  request_id: "web-2",
  payload: { text: "hello bob" }
}));
```

也可以发应用层 ping：

```js
socket.send(JSON.stringify({
  kind: "ping",
  request_id: "web-3",
  payload: { from: "browser" }
}));
```

## 生产提醒

这个工程刻意把“单机高性能 WebSocket 核心”做好，已内置：

- **JWT 鉴权**：HS256 / RS256 / EdDSA，无状态校验，见上文「鉴权」小节
- **多租户隔离**：基于 JWT `tenant_id`，主题与私聊均按租户命名空间隔离
- **IP 级限流**：并发上限 + 新建连接速率双维度，见上文「IP 限流」小节
- **运维端点**：`/healthz`、`/readyz`、`/metrics`（Prometheus 格式）

生产环境通常还需要在外部接入：

- **TLS 终止**：建议由 Nginx / Envoy / ALB 反代终止 TLS，应用层保持纯 WebSocket。反代示例：
  ```nginx
  location /ws {
      proxy_pass http://127.0.0.1:8080;
      proxy_http_version 1.1;
      proxy_set_header Upgrade $http_upgrade;
      proxy_set_header Connection "upgrade";
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Real-IP $remote_addr;
      # 避免把 token 写进 access log
      proxy_set_header X-Forwarded-Proto $scheme;
  }
  ```
  在反代后请设置 `WS_TRUST_PROXY_HEADERS=true` 以解析真实客户端 IP。

- **多实例广播**：单机核心定位下未内置。需要横向扩展时，可把广播抽象成 trait（默认内存实现，可替换为 Redis Pub/Sub / NATS / Kafka），不必改动协议层。
- **告警规则与压测基线**：见下文 Grafana 小节，按面板里的趋势线设置阈值告警。

## 业务系统对接指南

rust-ws-server 作为消息网关，位于业务系统与终端客户端之间。典型的对接模式涉及三个角色：

```
┌─────────────────┐
│  认证服务 (Issuer) │  ← 持有 JWT 签名密钥，为接入方签发 token
└────────┬────────┘
         │ 签发 token（REST / gRPC）
         ▼
┌─────────────────┐      HTTP API / WebSocket       ┌──────────────────┐
│ 业务后端 (Pusher) │ ──────────────────────────────→ │ rust-ws-server   │
│                 │ ←────────────────────────────── │                  │
└─────────────────┘     订阅/接收（可选）             └────────┬─────────┘
                                                            │ WebSocket
                                                            ▼
                                                     ┌──────────────────┐
                                                     │ 终端客户端 (App)  │
                                                     └──────────────────┘
```

- **认证服务**（Issuer）：业务系统自己的鉴权服务，持有 JWT 签名密钥（如 HS256 secret 或 RS256 私钥）。用户登录后签发包含 `sub`（client_id）和 `tenant_id` 的 JWT。
- **业务后端**（Pusher）：需要向在线客户端推送消息的后台服务（如订单状态变更、IM 消息转发）。通过 **HTTP API** 或 **WebSocket 客户端** 两种方式接入。
- **终端客户端**：浏览器/App，通过 WebSocket 连接 rust-ws-server，用认证服务签发的 token 握手。

### 方式一：HTTP API 推送（推荐）

业务后端无需维护 WebSocket 长连接，通过 REST 接口即可推送消息。适用于大多数服务端推送场景。

#### 认证

HTTP API 使用 Bearer token 鉴权，与服务端 JWT 鉴权共用同一套密钥和校验逻辑：

```bash
Authorization: Bearer <jwt-token>
```

> JWT token 的签发方式见上文「Token 生成」小节。业务后端通常由认证服务统一签发 token。

#### 端点

**`POST /api/publish`** — 向指定 topic 广播消息

请求体：
```json
{
  "topic": "order-events",
  "request_id": "req-001",
  "payload": { "order_id": "12345", "status": "shipped" }
}
```

| 字段 | 必需 | 说明 |
|------|------|------|
| `topic` | 否 | 目标主题，缺省 `"public"` |
| `request_id` | 否 | 请求追踪 ID，透传给接收方 |
| `payload` | 是 | 任意 JSON 值，透传给所有订阅该 topic 的 WS 客户端 |

成功响应（200）：
```json
{ "receivers": 3 }
```

`receivers` 表示实际送达的在线客户端数量；为 0 时服务端同时递增 `ws_messages_dropped_total` 计数器。

**`POST /api/direct`** — 向指定 client_id 发送私聊消息

请求体：
```json
{
  "to": "bob",
  "request_id": "req-002",
  "payload": { "text": "hello bob" }
}
```

| 字段 | 必需 | 说明 |
|------|------|------|
| `to` | 是 | 目标客户端 `client_id`。限定同租户，跨租户视为不存在 |
| `request_id` | 否 | 请求追踪 ID |
| `payload` | 是 | 任意 JSON 值 |

成功响应（200）：
```json
{ "ok": true }
```

目标不在线响应（404）：
```json
{ "ok": false, "error": "client_not_found" }
```

#### HTTP API 错误码

| 状态码 | 场景 |
|--------|------|
| 200 | 成功 |
| 401 | 缺少或无效的 Authorization header / JWT |
| 404 | direct 目标不在线或跨租户 |
| 500 | 消息编码失败 |

### 方式二：业务后端作为 WebSocket 客户端

业务后端需要**双向通信**（如接收客户端上行消息、订阅多个 topic）时，可以直接作为 WS 客户端接入。此时业务后端与终端客户端的角色完全一致。

**Rust 示例**（使用 `tokio-tungstenite`）：

```rust
use tokio_tungstenite::connect_async;
use futures_util::SinkExt;

// 用认证服务签发的 token 连接
let token = "eyJhbGciOiJIUzI1NiIs...";
let url = format!("ws://127.0.0.1:8080/ws?topic=order-events&token={token}");
let (mut ws, _) = connect_async(&url).await?;

// 接收消息
while let Some(Ok(msg)) = ws.next().await {
    // 出站统一为 MessagePack binary
    if let Message::Binary(bytes) = msg {
        let event: ServerEvent = rmp_serde::from_slice(&bytes)?;
        // 处理 message / direct_message 事件
    }
}
```

**Go 示例**（使用 `gorilla/websocket`）：

```go
import "github.com/gorilla/websocket"

token := "eyJhbGciOiJIUzI1NiIs..."
url := "ws://127.0.0.1:8080/ws?topic=order-events&token=" + token
conn, _, err := websocket.DefaultDialer.Dial(url, nil)
// 读取消息...
```

### 从业务系统签发 JWT

业务系统（认证服务）用自己的密钥签发 JWT，不依赖 rust-ws-server 的 `mint-token` 工具。

**Python 示例**（HS256）：

```python
import jwt
import time

secret = "your-hmac-secret"  # 与服务端 WS_JWT_SECRET 一致
claims = {
    "sub": "backend-svc-01",       # 推送方的 client_id
    "tenant_id": "company-a",      # 目标租户
    "exp": int(time.time()) + 3600
}
token = jwt.encode(claims, secret, algorithm="HS256")
```

**Python 示例**（RS256，推荐生产使用）：

```python
import jwt
import time

with open("private-key.pem") as f:
    private_key = f.read()

claims = {
    "sub": "backend-svc-01",
    "tenant_id": "company-a",
    "exp": int(time.time()) + 3600
}
token = jwt.encode(claims, private_key, algorithm="RS256")
```

服务端配置对应公钥：
```bash
WS_JWT_PUBLIC_KEY="$(cat public-key.pem)"
```

**Go 示例**（HS256）：

```go
import "github.com/golang-jwt/jwt/v5"

claims := jwt.MapClaims{
    "sub":       "backend-svc-01",
    "tenant_id": "company-a",
    "exp":       time.Now().Add(1 * time.Hour).Unix(),
}
token, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).
    SignedString([]byte("your-hmac-secret"))
```

### 端到端流程

```
1. 用户登录 → 认证服务签发 JWT（含 sub=user-123, tenant_id=company-a）
2. 终端客户端用 token 连接 rust-ws-server，订阅 topic "notifications"
3. 业务后端（如订单系统）状态变更，向 /api/publish 发 POST：
     Authorization: Bearer <后台自己的 token，tenant_id=company-a>
     { "topic": "notifications", "payload": { "order_id": "X", "status": "done" } }
4. rust-ws-server 校验 tenant_id，投递到 company-a:notifications 主题
5. 终端客户端收到 MessagePack 消息，展示通知
```

关键约束：
- 推送方 token 的 `tenant_id` 决定了能推送到的租户范围。跨租户推送被路由隔离。
- `direct` 消息的 `to` 字段也限定同租户，跨租户返回 `client_not_found`。
- API 不提供租户间消息转发——这是业务层该做的事（如拆成两次 publish）。

## Grafana

仓库提供 `grafana/dashboard.json`，覆盖连接数、消息吞吐、丢消息、协议错误、鉴权/IP 拒绝等指标。

导入步骤：

1. 在 Grafana 中新建 Prometheus datasource，指向抓取 `/metrics` 的 Prometheus 实例。
2. `Dashboards → Import → Upload JSON file`，选择 `grafana/dashboard.json`。
3. 在 datasource 变量中选择上一步创建的 Prometheus。
4. 默认时间窗口为最近 1 小时，刷新间隔 10s。
