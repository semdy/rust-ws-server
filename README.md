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
- JSON 协议、结构化日志、优雅退出
- 集成测试覆盖发布、广播、限流路径

## 运行

```bash
cargo run --release
```

默认监听 `0.0.0.0:8080`，WebSocket 地址为：

```text
ws://127.0.0.1:8080/ws?topic=public&client_id=alice
```

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
cargo run --release
```

## 客户端消息

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
ws://127.0.0.1:8080/ws?topic=public&client_id=web-demo
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

这个工程刻意把“单机高性能 WebSocket 核心”做好，但生产环境通常还需要接入：

- TLS 终止，例如 Nginx、Envoy、ALB 或 rustls
- 鉴权与租户隔离，例如 JWT、mTLS、签名 URL
- 多实例广播，例如 Redis Pub/Sub、NATS、Kafka 或自研网关层
- IP 级限流、黑名单和风控，配合当前连接级消息限流
- Prometheus/Grafana 告警规则和压测基线
