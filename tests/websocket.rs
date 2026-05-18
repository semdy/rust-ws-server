use std::{net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use rust_ws_server::{config::Config, server, state::AppState};
use serde_json::json;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};

async fn spawn_server(max_connections: usize) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let config = Config {
        bind_addr: addr,
        max_connections,
        client_queue_capacity: 16,
        topic_channel_capacity: 32,
        max_text_bytes: 64 * 1024,
        idle_timeout: Duration::from_secs(5),
        heartbeat_interval: Duration::from_secs(60),
        json_logs: false,
    };
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

#[tokio::test]
async fn broadcasts_messages_to_topic_subscribers() {
    let (addr, handle) = spawn_server(10).await;
    let url = format!("ws://{addr}/ws?topic=test&client_id=a");
    let (mut a, _) = connect_async(&url).await.unwrap();
    let (mut b, _) = connect_async(format!("ws://{addr}/ws?topic=test&client_id=b"))
        .await
        .unwrap();

    let _ = a.next().await.unwrap().unwrap();
    let _ = b.next().await.unwrap().unwrap();

    a.send(text_message(
        json!({"kind":"publish","request_id":"r1","payload":{"text":"hello"}}),
    ))
    .await
    .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    assert!(received.contains("\"kind\":\"message\""));
    assert!(received.contains("\"hello\""));

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

    let _ = a.next().await.unwrap().unwrap();
    let _ = b.next().await.unwrap().unwrap();
    let _ = c.next().await.unwrap().unwrap();

    a.send(text_message(
        json!({"kind":"direct","to":"b","request_id":"d1","payload":{"text":"secret"}}),
    ))
    .await
    .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    assert!(received.contains("\"kind\":\"direct_message\""));
    assert!(received.contains("\"from\":\"a\""));
    assert!(received.contains("\"to\":\"b\""));
    assert!(received.contains("\"secret\""));

    let not_received = tokio::time::timeout(Duration::from_millis(100), c.next()).await;
    assert!(not_received.is_err());

    handle.abort();
}

#[tokio::test]
async fn rejects_connections_over_limit() {
    let (addr, handle) = spawn_server(1).await;
    let (mut first, _) = connect_async(format!("ws://{addr}/ws?topic=limit"))
        .await
        .unwrap();
    let _ = first.next().await.unwrap().unwrap();

    let second = connect_async(format!("ws://{addr}/ws?topic=limit")).await;
    assert!(second.is_err());

    handle.abort();
}
