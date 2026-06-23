use std::{net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rust_ws_server::{config::Config, server, state::AppState};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const TEST_SECRET: &str = "test-secret";

#[derive(Serialize, Deserialize)]
struct TestClaims {
    sub: String,
    tenant_id: Option<String>,
    exp: i64,
    iss: Option<String>,
}

fn now_plus(secs: i64) -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + secs
}

fn sign_token(claims: &TestClaims) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap()
}

fn token_for(sub: &str, tenant_id: Option<&str>) -> String {
    sign_token(&TestClaims {
        sub: sub.to_owned(),
        tenant_id: tenant_id.map(str::to_owned),
        exp: now_plus(3600),
        iss: None,
    })
}

fn base_config() -> Config {
    Config {
        bind_addr: "0.0.0.0:0".parse::<SocketAddr>().unwrap(),
        max_connections: 10,
        client_queue_capacity: 16,
        topic_channel_capacity: 32,
        max_text_bytes: 64 * 1024,
        max_messages_per_second: 100,
        message_burst: 200,
        idle_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_secs(60),
        json_logs: false,
        jwt_secret: None,
        jwt_public_key: None,
        jwt_issuer: None,
        ip_max_concurrent: None,
        ip_connection_rate: None,
        ip_rate_burst: None,
        trust_proxy_headers: false,
        tenant_max_connections: None,
        tenant_max_messages_per_second: None,
        tenant_message_burst: None,
    }
}

async fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn spawn_server(config: Config) -> (SocketAddr, JoinHandle<()>) {
    let addr = config.bind_addr;
    let state = AppState::new(config);
    let app = server::router(state);
    let listener = TcpListener::bind(addr).await.unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (addr, handle)
}

async fn spawn_default_server() -> (SocketAddr, JoinHandle<()>) {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    spawn_server(config).await
}

async fn spawn_auth_server() -> (SocketAddr, JoinHandle<()>) {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.jwt_secret = Some(TEST_SECRET.into());
    spawn_server(config).await
}

fn text_message(value: serde_json::Value) -> Message {
    Message::Text(value.to_string().into())
}

fn binary_message(value: serde_json::Value) -> Message {
    Message::Binary(rmp_serde::to_vec_named(&value).unwrap().into())
}

async fn next_server_event<S>(socket: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match frame {
        Message::Binary(bytes) => rmp_serde::from_slice(&bytes).unwrap(),
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[tokio::test]
async fn broadcasts_messages_to_topic_subscribers() {
    let (addr, handle) = spawn_default_server().await;
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=a"))
        .await
        .unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=b"))
        .await
        .unwrap();

    let _ = next_server_event(&mut a).await;
    let _ = next_server_event(&mut b).await;

    a.send(text_message(
        json!({"kind":"publish","request_id":"r1","payload":{"text":"hello"}}),
    ))
    .await
    .unwrap();

    let received = next_server_event(&mut b).await;
    assert_eq!(received["kind"], "message");
    assert_eq!(received["payload"]["text"], "hello");

    handle.abort();
}

#[tokio::test]
async fn sends_direct_messages_to_one_client() {
    let (addr, handle) = spawn_default_server().await;
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=a"))
        .await
        .unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=b"))
        .await
        .unwrap();
    let (mut c, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=c"))
        .await
        .unwrap();

    let _ = next_server_event(&mut a).await;
    let _ = next_server_event(&mut b).await;
    let _ = next_server_event(&mut c).await;

    a.send(text_message(
        json!({"kind":"direct","to":"b","request_id":"d1","payload":{"text":"secret"}}),
    ))
    .await
    .unwrap();

    let received = next_server_event(&mut b).await;
    assert_eq!(received["kind"], "direct_message");
    assert_eq!(received["from"], "a");
    assert_eq!(received["to"], "b");
    assert_eq!(received["payload"]["text"], "secret");

    let not_received = tokio::time::timeout(Duration::from_millis(100), c.next()).await;
    assert!(not_received.is_err());

    handle.abort();
}

#[tokio::test]
async fn rate_limits_noisy_clients() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.max_messages_per_second = 1;
    config.message_burst = 1;
    let (addr, handle) = spawn_server(config).await;

    let (mut client, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=noisy"))
        .await
        .unwrap();
    let _ = next_server_event(&mut client).await;

    for seq in 0..3 {
        client
            .send(text_message(
                json!({"kind":"publish","request_id":format!("r{seq}"),"payload":{"text":"too fast"}}),
            ))
            .await
            .unwrap();
    }

    let mut saw_rate_limit = false;
    for _ in 0..4 {
        let Some(frame) = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .unwrap()
        else {
            break;
        };
        let value = match frame.unwrap() {
            Message::Binary(bytes) => rmp_serde::from_slice::<Value>(&bytes).unwrap(),
            Message::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
            _ => continue,
        };
        if value["code"] == "rate_limited" {
            saw_rate_limit = true;
            break;
        }
    }
    assert!(saw_rate_limit);

    handle.abort();
}

#[tokio::test]
async fn rejects_connections_over_limit() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.max_connections = 1;
    let (addr, handle) = spawn_server(config).await;

    let (mut first, _) = connect_async(format!("ws://{addr}/ws?topic=limit"))
        .await
        .unwrap();
    let _ = next_server_event(&mut first).await;

    let second = connect_async(format!("ws://{addr}/ws?topic=limit")).await;
    assert!(second.is_err());

    handle.abort();
}

#[tokio::test]
async fn accepts_msgpack_client_messages() {
    let (addr, handle) = spawn_default_server().await;
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=a"))
        .await
        .unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=b"))
        .await
        .unwrap();

    let _ = next_server_event(&mut a).await;
    let _ = next_server_event(&mut b).await;

    a.send(binary_message(
        json!({"kind":"publish","request_id":"r1","payload":{"text":"hello msgpack"}}),
    ))
    .await
    .unwrap();

    let received = next_server_event(&mut b).await;
    assert_eq!(received["kind"], "message");
    assert_eq!(received["payload"]["text"], "hello msgpack");

    handle.abort();
}

#[tokio::test]
async fn rejects_connection_without_token_when_auth_enabled() {
    let (addr, handle) = spawn_auth_server().await;
    let result = connect_async(format!("ws://{addr}/ws?topic=test&client_id=a")).await;
    assert!(result.is_err());
    handle.abort();
}

#[tokio::test]
async fn url_tenant_id_isolates_topics_in_dev_mode() {
    // JWT disabled — `?tenant_id=` on the URL is the source of tenant identity.
    let (addr, handle) = spawn_default_server().await;
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=room&client_id=a&tenant_id=t1"))
        .await
        .unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=room&client_id=b&tenant_id=t2"))
        .await
        .unwrap();
    let _ = next_server_event(&mut a).await;
    let _ = next_server_event(&mut b).await;

    a.send(text_message(
        json!({"kind":"publish","request_id":"r1","payload":{"text":"t1 only"}}),
    ))
    .await
    .unwrap();

    // Same topic name, different tenant — b must not see a's message.
    let not_received = tokio::time::timeout(Duration::from_millis(200), b.next()).await;
    assert!(not_received.is_err());
    handle.abort();
}

#[tokio::test]
async fn jwt_ignores_url_tenant_id() {
    // JWT enabled — the claim's tenant_id wins; URL `?tenant_id=` is silently ignored.
    let (addr, handle) = spawn_auth_server().await;
    let token = token_for("alice", Some("t1"));
    // URL claims tenant_id=t2, but JWT says t1. Identity must be t1.
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token}&tenant_id=t2"))
        .await
        .unwrap();
    let ready = next_server_event(&mut a).await;
    assert_eq!(ready["client_id"], "alice");
    handle.abort();
}

#[tokio::test]
async fn accepts_connection_with_valid_jwt() {
    let (addr, handle) = spawn_auth_server().await;
    let token = token_for("alice", None);
    let url = format!("ws://{addr}/ws?topic=test&token={token}");
    let (mut a, _) = connect_async(&url).await.unwrap();
    let ready = next_server_event(&mut a).await;
    // The `sub` claim overrides any client_id; ready echoes the JWT-derived identity.
    assert_eq!(ready["kind"], "ready");
    assert_eq!(ready["client_id"], "alice");
    handle.abort();
}

#[tokio::test]
async fn isolates_topics_between_tenants() {
    let (addr, handle) = spawn_auth_server().await;
    let token_a = token_for("alice", Some("t1"));
    let token_b = token_for("bob", Some("t2"));
    // Both connect to the same client-facing topic name "room-a".
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=room-a&token={token_a}"))
        .await
        .unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=room-a&token={token_b}"))
        .await
        .unwrap();
    let _ = next_server_event(&mut a).await;
    let _ = next_server_event(&mut b).await;

    a.send(text_message(
        json!({"kind":"publish","request_id":"r1","payload":{"text":"only t1 sees this"}}),
    ))
    .await
    .unwrap();

    // Bob (t2) should NOT receive the message — different tenant.
    let not_received = tokio::time::timeout(Duration::from_millis(200), b.next()).await;
    assert!(not_received.is_err());
    handle.abort();
}

#[tokio::test]
async fn blocks_direct_messages_across_tenants() {
    let (addr, handle) = spawn_auth_server().await;
    let token_a = token_for("alice", Some("t1"));
    let token_b = token_for("bob", Some("t2"));
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token_a}"))
        .await
        .unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token_b}"))
        .await
        .unwrap();
    let _ = next_server_event(&mut a).await;
    let _ = next_server_event(&mut b).await;

    // Alice (t1) tries to direct-message bob (t2). Cross-tenant must be blocked.
    a.send(text_message(
        json!({"kind":"direct","to":"bob","request_id":"d1","payload":{"text":"cross-tenant"}}),
    ))
    .await
    .unwrap();

    // Alice should get a client_not_found error.
    let err = next_server_event(&mut a).await;
    assert_eq!(err["code"], "client_not_found");
    // Bob should not receive anything.
    let not_received = tokio::time::timeout(Duration::from_millis(200), b.next()).await;
    assert!(not_received.is_err());
    handle.abort();
}

#[tokio::test]
async fn rejects_connection_over_ip_concurrent_limit() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.ip_max_concurrent = Some(1);
    let (addr, handle) = spawn_server(config).await;

    let (mut first, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=a"))
        .await
        .unwrap();
    let _ = next_server_event(&mut first).await;

    let second = connect_async(format!("ws://{addr}/ws?topic=test&client_id=b")).await;
    assert!(second.is_err());
    handle.abort();
}

#[tokio::test]
async fn rejects_connection_over_ip_rate_limit() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.ip_connection_rate = Some(1);
    config.ip_rate_burst = Some(1);
    let (addr, handle) = spawn_server(config).await;

    // First connection consumes the single token.
    let (_first, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=a"))
        .await
        .unwrap();

    // Immediate second connection from the same IP must be rejected.
    let second = connect_async(format!("ws://{addr}/ws?topic=test&client_id=b")).await;
    assert!(second.is_err());
    handle.abort();
}

#[tokio::test]
async fn rejects_connection_over_tenant_cap() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.jwt_secret = Some(TEST_SECRET.into());
    config.tenant_max_connections = Some(1);
    let (addr, handle) = spawn_server(config).await;

    // Tenant t1: first connection admitted.
    let token_a = token_for("alice", Some("t1"));
    let (_first, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token_a}"))
        .await
        .unwrap();

    // Tenant t1: second connection must be rejected (cap = 1).
    let token_b = token_for("bob", Some("t1"));
    let second = connect_async(format!("ws://{addr}/ws?topic=test&token={token_b}")).await;
    assert!(second.is_err());
    handle.abort();
}

#[tokio::test]
async fn tenant_cap_does_not_affect_other_tenants() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.jwt_secret = Some(TEST_SECRET.into());
    config.tenant_max_connections = Some(1);
    let (addr, handle) = spawn_server(config).await;

    // Tenant t1 fills its cap.
    let token_t1 = token_for("alice", Some("t1"));
    let (_first, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token_t1}"))
        .await
        .unwrap();

    // Tenant t2 must still be admitted — its cap is independent.
    let token_t2 = token_for("bob", Some("t2"));
    let (mut second, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token_t2}"))
        .await
        .unwrap();
    let ready = next_server_event(&mut second).await;
    assert_eq!(ready["client_id"], "bob");
    handle.abort();
}

#[tokio::test]
async fn tenant_rate_limit_drops_messages_but_keeps_connection() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.jwt_secret = Some(TEST_SECRET.into());
    config.tenant_max_messages_per_second = Some(1);
    config.tenant_message_burst = Some(1);
    // Raise the per-connection rate so it doesn't fire first and close the socket.
    config.max_messages_per_second = 100;
    config.message_burst = 100;
    let (addr, handle) = spawn_server(config).await;

    let token = token_for("alice", Some("noisy"));
    let (mut a, _) = connect_async(format!("ws://{addr}/ws?topic=test&token={token}"))
        .await
        .unwrap();
    let _ = next_server_event(&mut a).await;

    // Send 3 messages rapidly. Tenant bucket capacity is 1, so messages 2 and 3 must be
    // rejected with `tenant_rate_limited` — but the connection must stay open.
    for seq in 0..3 {
        a.send(text_message(
            json!({"kind":"publish","request_id":format!("r{seq}"),"payload":{"text":"x"}}),
        ))
        .await
        .unwrap();
    }

    let mut rejections = 0;
    // Drain frames until the socket goes quiet. Only 2 `tenant_rate_limited` errors are
    // expected (message 1 admitted, messages 2 and 3 rejected) — we read until timeout
    // rather than fixing a count, then assert we saw at least one rejection.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), a.next()).await {
            Ok(Some(Ok(frame))) => match frame {
                Message::Binary(bytes) => {
                    let value: Value = rmp_serde::from_slice(&bytes).unwrap();
                    if value["code"] == "tenant_rate_limited" {
                        rejections += 1;
                    }
                }
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["code"] == "tenant_rate_limited" {
                        rejections += 1;
                    }
                }
                Message::Close(_) => panic!("connection was closed by tenant rate limiter"),
                _ => continue,
            },
            _ => break,
        }
    }
    assert!(rejections >= 1, "expected at least one tenant_rate_limited");
    handle.abort();
}

#[tokio::test]
async fn tenant_rate_limit_does_not_affect_other_tenants() {
    let mut config = base_config();
    config.bind_addr = free_addr().await;
    config.jwt_secret = Some(TEST_SECRET.into());
    config.tenant_max_messages_per_second = Some(1);
    config.tenant_message_burst = Some(1);
    config.max_messages_per_second = 100;
    config.message_burst = 100;
    let (addr, handle) = spawn_server(config).await;

    // Tenant t1 connects two clients to the same topic so the receiver can observe delivery.
    let token_a = token_for("alice", Some("t1"));
    let token_b = token_for("bob", Some("t2"));
    let (mut a_t1, _) = connect_async(format!("ws://{addr}/ws?topic=room&token={token_a}"))
        .await
        .unwrap();
    let (mut b_t2, _) = connect_async(format!("ws://{addr}/ws?topic=room&token={token_b}"))
        .await
        .unwrap();
    let _ = next_server_event(&mut a_t1).await;
    let _ = next_server_event(&mut b_t2).await;

    // t1 floods past its cap — messages get dropped for t1, but t1 and t2 stay connected.
    for seq in 0..3 {
        a_t1
            .send(text_message(
                json!({"kind":"publish","request_id":format!("r{seq}"),"payload":{"text":"flood"}}),
            ))
            .await
            .unwrap();
    }

    // Probe t2's connection health with an app-level ping. If t1's flooding had cross-tenant
    // side effects (it mustn't), t2's socket would be closed and we'd see no pong.
    b_t2
        .send(text_message(
            json!({"kind":"ping","request_id":"t2-probe","payload":null}),
        ))
        .await
        .unwrap();

    let mut got_pong = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), b_t2.next()).await {
            Ok(Some(Ok(frame))) => {
                let value = match frame {
                    Message::Binary(bytes) => rmp_serde::from_slice::<Value>(&bytes).unwrap(),
                    Message::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
                    Message::Close(_) => break,
                    _ => continue,
                };
                if value["kind"] == "pong" {
                    got_pong = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(got_pong, "t2 should still respond after t1 flooded its own tenant bucket");
    handle.abort();
}
