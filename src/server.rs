use std::sync::Arc;

use axum::{
    Router,
    extract::{
        Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, mpsc},
    time::{interval, sleep},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{debug, info, warn};

use crate::{
    protocol::{ClientEvent, ServerEvent, encode_server_event, parse_client_event},
    state::SharedState,
};

#[derive(Debug, Deserialize)]
struct WsQuery {
    #[serde(default = "default_topic")]
    topic: String,
    client_id: Option<String>,
}

fn default_topic() -> String {
    "public".to_owned()
}

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn serve(state: SharedState) -> anyhow::Result<()> {
    let listener = TcpListener::bind(state.config.bind_addr).await?;
    info!(addr = %state.config.bind_addr, "websocket server listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn readyz() -> &'static str {
    "ready\n"
}

async fn metrics(State(state): State<SharedState>) -> String {
    state.metrics.render_prometheus()
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<SharedState>,
) -> Response {
    let permit = match state.connection_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            state.metrics.connection_rejected();
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "connection limit reached\n",
            )
                .into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_socket(socket, state, query, permit))
}

async fn handle_socket(
    socket: WebSocket,
    state: SharedState,
    query: WsQuery,
    _permit: OwnedSemaphorePermit,
) {
    let client_id = query.client_id.unwrap_or_else(|| "anonymous".to_owned());
    let topic = normalize_topic(&query.topic);
    state.metrics.connection_accepted();
    info!(%client_id, %topic, "websocket connected");

    let result = connection_loop(socket, state.clone(), topic.clone(), client_id.clone()).await;
    if let Err(err) = result {
        warn!(%client_id, %topic, error = %err, "websocket connection ended with error");
    }

    state.metrics.connection_closed();
    debug!(%client_id, %topic, "websocket closed");
}

async fn connection_loop(
    socket: WebSocket,
    state: SharedState,
    topic: String,
    client_id: String,
) -> anyhow::Result<()> {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (client_tx, mut client_rx) = mpsc::channel::<Message>(state.config.client_queue_capacity);
    let mut topic_rx = state.subscribe(&topic).await;
    let connection_id = state.next_connection_id();
    state
        .register_client(client_id.clone(), connection_id, client_tx.clone())
        .await;

    send_json(
        &client_tx,
        &encode_server_event(&ServerEvent::Ready {
            topic: &topic,
            client_id: &client_id,
        })?,
        &state,
    );

    let writer_state = state.clone();
    let writer = tokio::spawn(async move {
        let mut heartbeat = interval(writer_state.config.heartbeat_interval);
        heartbeat.tick().await;
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if ws_sender.send(Message::Ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
                message = client_rx.recv() => {
                    match message {
                        Some(message) => {
                            if ws_sender.send(message).await.is_err() {
                                break;
                            }
                            writer_state.metrics.message_out();
                        }
                        None => break,
                    }
                }
            }
        }
    });

    let fanout_state = state.clone();
    let fanout_tx = client_tx.clone();
    let fanout = tokio::spawn(async move {
        loop {
            match topic_rx.recv().await {
                Ok(raw) => send_json(&fanout_tx, &raw, &fanout_state),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    fanout_state.metrics.message_dropped();
                    warn!(skipped, "client lagged behind topic broadcast");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let idle_timeout = state.config.idle_timeout;
    let reader = async {
        loop {
            tokio::select! {
                frame = ws_receiver.next() => {
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            state.metrics.message_in();
                            if text.len() > state.config.max_text_bytes {
                                state.metrics.protocol_error();
                                send_error(&client_tx, "message_too_large", "text message exceeds configured limit", &state);
                                continue;
                            }
                            handle_text(&state, &client_tx, &topic, &client_id, &text).await;
                        }
                        Some(Ok(Message::Binary(_))) => {
                            state.metrics.protocol_error();
                            send_error(&client_tx, "unsupported_message", "binary messages are not supported", &state);
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = client_tx.try_send(Message::Pong(payload));
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Err(err)) => return Err(anyhow::anyhow!(err)),
                    }
                }
                _ = sleep(idle_timeout) => {
                    let _ = client_tx.try_send(Message::Close(Some(CloseFrame {
                        code: axum::extract::ws::close_code::NORMAL,
                        reason: "idle timeout".into(),
                    })));
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    let reader_result = reader.await;
    fanout.abort();
    writer.abort();
    let _ = fanout.await;
    let _ = writer.await;
    state.unregister_client(&client_id, connection_id).await;
    reader_result
}

async fn handle_text(
    state: &SharedState,
    client_tx: &mpsc::Sender<Message>,
    default_topic: &str,
    client_id: &str,
    text: &str,
) {
    match parse_client_event(text) {
        Ok(ClientEvent::Publish {
            topic,
            request_id,
            payload,
        }) => {
            let topic = topic
                .as_deref()
                .map(normalize_topic)
                .unwrap_or_else(|| default_topic.to_owned());
            match encode_server_event(&ServerEvent::Message {
                topic: &topic,
                from: client_id,
                request_id: request_id.as_deref(),
                payload: &payload,
            }) {
                Ok(encoded) => {
                    let receivers = state.publish(&topic, Arc::<str>::from(encoded)).await;
                    if receivers == 0 {
                        state.metrics.message_dropped();
                    }
                }
                Err(_) => {
                    state.metrics.protocol_error();
                    send_error(
                        client_tx,
                        "encode_failed",
                        "failed to encode message",
                        state,
                    );
                }
            }
        }
        Ok(ClientEvent::Direct {
            to,
            request_id,
            payload,
        }) => match encode_server_event(&ServerEvent::DirectMessage {
            from: client_id,
            to: &to,
            request_id: request_id.as_deref(),
            payload: &payload,
        }) {
            Ok(encoded) => {
                if !state.send_to_client(&to, Arc::<str>::from(encoded)).await {
                    state.metrics.message_dropped();
                    send_error(
                        client_tx,
                        "client_not_found",
                        "target client is not online",
                        state,
                    );
                }
            }
            Err(_) => {
                state.metrics.protocol_error();
                send_error(
                    client_tx,
                    "encode_failed",
                    "failed to encode direct message",
                    state,
                );
            }
        },
        Ok(ClientEvent::Ping {
            request_id,
            payload,
        }) => {
            match encode_server_event(&ServerEvent::Pong {
                request_id: request_id.as_deref(),
                payload: payload.as_ref(),
            }) {
                Ok(encoded) => send_json(client_tx, &encoded, state),
                Err(_) => send_error(client_tx, "encode_failed", "failed to encode pong", state),
            }
        }
        Err(_) => {
            state.metrics.protocol_error();
            send_error(
                client_tx,
                "invalid_json",
                "message must match the websocket JSON protocol",
                state,
            );
        }
    }
}

fn send_error(client_tx: &mpsc::Sender<Message>, code: &str, message: &str, state: &SharedState) {
    if let Ok(encoded) = encode_server_event(&ServerEvent::Error { code, message }) {
        send_json(client_tx, &encoded, state);
    }
}

fn send_json(client_tx: &mpsc::Sender<Message>, text: &str, state: &SharedState) {
    match client_tx.try_send(Message::Text(text.to_owned())) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => state.metrics.message_dropped(),
        Err(mpsc::error::TrySendError::Closed(_)) => state.metrics.message_dropped(),
    }
}

fn normalize_topic(topic: &str) -> String {
    let topic = topic.trim().trim_start_matches('/');
    if topic.is_empty() {
        "public".to_owned()
    } else {
        topic.chars().take(128).collect()
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => warn!(error = %err, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
