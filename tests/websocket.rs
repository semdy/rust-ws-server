use std::{net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rust_ws_server::{config::Config, server, state::AppState};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};

async fn spawn_server(max_connections: usize) -> (SocketAddr, JoinHandle<()>) {
    spawn_server_with_config(Config {
        bind_addr: free_addr().await,
        max_connections,
        client_queue_capacity: 16,
        topic_channel_capacity: 32,
        max_text_bytes: 64 * 1024,
        max_messages_per_second: 100,
        message_burst: 200,
        idle_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_secs(60),
        json_logs: false,
    })
    .await
}

async fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn spawn_server_with_config(config: Config) -> (SocketAddr, JoinHandle<()>) {
    let addr = config.bind_addr;
    let state = AppState::new(config);
    let app = server::router(state);
    let listener = TcpListener::bind(addr).await.unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, handle)
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
    let (addr, handle) = spawn_server(10).await;
    let url = format!("ws://{addr}/ws?topic=test&client_id=a");
    let (mut a, _) = connect_async(&url).await.unwrap();
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
    let (addr, handle) = spawn_server(10).await;
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
    let (addr, handle) = spawn_server_with_config(Config {
        bind_addr: free_addr().await,
        max_connections: 10,
        client_queue_capacity: 16,
        topic_channel_capacity: 32,
        max_text_bytes: 64 * 1024,
        max_messages_per_second: 1,
        message_burst: 1,
        idle_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_secs(60),
        json_logs: false,
    })
    .await;

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
    let (addr, handle) = spawn_server(1).await;
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
    let (addr, handle) = spawn_server(10).await;
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
