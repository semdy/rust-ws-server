use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{
        ConnectInfo, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
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
    auth::AuthIdentity,
    ip_limiter::{IpPermit, IpRejection},
    protocol::{
        ClientEvent, ServerEvent, encode_server_event, parse_client_event_binary,
        parse_client_event_text,
    },
    state::{OutboundMessage, SharedState, tenant_topic},
};

#[derive(Debug, Deserialize)]
struct WsQuery {
    #[serde(default = "default_topic")]
    topic: String,
    /// Ignored when JWT auth is enabled (the `sub` claim overrides it).
    client_id: Option<String>,
    /// JWT. Required when auth is enabled.
    token: Option<String>,
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
    if state.auth.is_none() {
        warn!("JWT auth disabled — set WS_JWT_SECRET or WS_JWT_PUBLIC_KEY to require authentication");
    }
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
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
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ip = resolve_client_ip(peer, &headers, state.config.trust_proxy_headers);

    // IP-level admission (before the global semaphore so rejected IPs don't consume slots).
    let ip_permit = match &state.ip_limiter {
        Some(limiter) => match limiter.try_acquire(ip) {
            Ok(permit) => Some(permit),
            Err(reason) => {
                state.metrics.ip_rejected();
                state.metrics.connection_rejected();
                let msg = match reason {
                    IpRejection::RateLimited => "ip connection rate limit exceeded\n",
                    IpRejection::ConcurrencyLimited => "ip concurrent connection limit exceeded\n",
                };
                return (StatusCode::TOO_MANY_REQUESTS, msg).into_response();
            }
        },
        None => None,
    };

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

    // JWT auth.
    let identity = match &state.auth {
        Some(verifier) => match query.token.as_deref() {
            Some(token) => match verifier.verify(token) {
                Ok(identity) => identity,
                Err(err) => {
                    state.metrics.auth_rejected();
                    state.metrics.connection_rejected();
                    debug!(error = %err, "jwt verification failed");
                    return (StatusCode::UNAUTHORIZED, "invalid token\n").into_response();
                }
            },
            None => {
                state.metrics.auth_rejected();
                state.metrics.connection_rejected();
                return (StatusCode::UNAUTHORIZED, "missing token\n").into_response();
            }
        },
        None => AuthIdentity {
            client_id: query
                .client_id
                .unwrap_or_else(|| "anonymous".to_owned()),
            tenant_id: "default".to_owned(),
        },
    };

    // Per-tenant concurrent-connection cap. Applied after auth so we know the tenant_id.
    // Rejected tenants don't hold the global connection slot — `permit` is dropped on return.
    let tenant_permit = match state.acquire_tenant_permit(&identity.tenant_id) {
        None => None,
        Some(Ok(p)) => Some(p),
        Some(Err(())) => {
            state.metrics.tenant_rejected();
            state.metrics.connection_rejected();
            return (
                StatusCode::TOO_MANY_REQUESTS,
                "tenant connection limit exceeded\n",
            )
                .into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_socket(
        socket,
        state,
        query.topic,
        identity,
        permit,
        ip_permit,
        tenant_permit,
    ))
}

fn resolve_client_ip(peer: SocketAddr, headers: &HeaderMap, trust_proxy: bool) -> IpAddr {
    if trust_proxy {
        if let Some(xff) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            && let Some(first) = xff.split(',').next()
            && let Ok(ip) = first.trim().parse::<IpAddr>()
        {
            return ip;
        }
        if let Some(xrip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok())
            && let Ok(ip) = xrip.trim().parse::<IpAddr>()
        {
            return ip;
        }
    }
    peer.ip()
}

async fn handle_socket(
    socket: WebSocket,
    state: SharedState,
    raw_topic: String,
    identity: AuthIdentity,
    _permit: OwnedSemaphorePermit,
    _ip_permit: Option<IpPermit>,
    _tenant_permit: Option<OwnedSemaphorePermit>,
) {
    let client_id = identity.client_id.clone();
    let tenant_id = identity.tenant_id.clone();
    let topic = normalize_topic(&raw_topic);
    let topic_key = tenant_topic(&tenant_id, &topic);
    state.metrics.connection_accepted();
    info!(%client_id, %tenant_id, %topic, "websocket connected");

    let result = connection_loop(
        socket,
        state.clone(),
        topic_key,
        tenant_id,
        client_id.clone(),
    )
    .await;
    if let Err(err) = result {
        warn!(%client_id, error = %err, "websocket connection ended with error");
    }

    state.metrics.connection_closed();
    debug!(%client_id, "websocket closed");
}

async fn connection_loop(
    socket: WebSocket,
    state: SharedState,
    topic_key: String,
    tenant_id: String,
    client_id: String,
) -> anyhow::Result<()> {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (client_tx, mut client_rx) =
        mpsc::channel::<OutboundMessage>(state.config.client_queue_capacity);
    let mut topic_rx = state.subscribe(&topic_key);
    let connection_id = state.next_connection_id();
    state.register_client(
        client_id.clone(),
        tenant_id.clone(),
        connection_id,
        client_tx.clone(),
    );

    send_encoded(
        &client_tx,
        encode_server_event(&ServerEvent::Ready {
            topic: extract_topic(&topic_key),
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
                        return Err(anyhow::anyhow!("failed to send websocket ping"));
                    }
                }
                message = client_rx.recv() => {
                    match message {
                        Some(message) => {
                            if ws_sender.send(message.into_ws_message()).await.is_err() {
                                return Err(anyhow::anyhow!("failed to send websocket message"));
                            }
                            writer_state.metrics.message_out();
                        }
                        None => break,
                    }
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    let fanout_state = state.clone();
    let fanout_tx = client_tx.clone();
    let fanout = tokio::spawn(async move {
        loop {
            match topic_rx.recv().await {
                Ok(raw) => {
                    if !send_encoded(&fanout_tx, raw, &fanout_state) {
                        return Err(anyhow::anyhow!("client send queue is full or closed"));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    fanout_state.metrics.message_dropped();
                    warn!(skipped, "client lagged behind topic broadcast");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    let idle_timeout = state.config.idle_timeout;
    let reader_state = state.clone();
    let reader_tx = client_tx.clone();
    let reader_topic_key = topic_key.clone();
    let reader_tenant = tenant_id.clone();
    let reader_client_id = client_id.clone();
    let reader = tokio::spawn(async move {
        let mut rate_limiter = RateLimiter::new(
            reader_state.config.max_messages_per_second,
            reader_state.config.message_burst,
        );
        loop {
            tokio::select! {
                frame = ws_receiver.next() => {
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            reader_state.metrics.message_in();
                            if !rate_limiter.allow() {
                                reader_state.metrics.protocol_error();
                                send_error(&reader_tx, "rate_limited", "message rate limit exceeded", &reader_state);
                                let _ = reader_tx.try_send(OutboundMessage::Close {
                                    code: axum::extract::ws::close_code::POLICY,
                                    reason: "rate limit exceeded",
                                });
                                break;
                            }
                            if text.len() > reader_state.config.max_text_bytes {
                                reader_state.metrics.protocol_error();
                                send_error(&reader_tx, "message_too_large", "text message exceeds configured limit", &reader_state);
                                continue;
                            }
                            if !reader_state.check_tenant_rate(&reader_tenant) {
                                reader_state.metrics.tenant_rate_rejected();
                                send_error(&reader_tx, "tenant_rate_limited", "tenant message rate exceeded", &reader_state);
                                continue;
                            }
                            handle_text(&reader_state, &reader_tx, &reader_topic_key, &reader_tenant, &reader_client_id, &text).await;
                        }
                        Some(Ok(Message::Binary(raw))) => {
                            reader_state.metrics.message_in();
                            if !rate_limiter.allow() {
                                reader_state.metrics.protocol_error();
                                send_error(&reader_tx, "rate_limited", "message rate limit exceeded", &reader_state);
                                let _ = reader_tx.try_send(OutboundMessage::Close {
                                    code: axum::extract::ws::close_code::POLICY,
                                    reason: "rate limit exceeded",
                                });
                                break;
                            }
                            if !reader_state.check_tenant_rate(&reader_tenant) {
                                reader_state.metrics.tenant_rate_rejected();
                                send_error(&reader_tx, "tenant_rate_limited", "tenant message rate exceeded", &reader_state);
                                continue;
                            }
                            handle_binary(&reader_state, &reader_tx, &reader_topic_key, &reader_tenant, &reader_client_id, &raw).await;
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = reader_tx.try_send(OutboundMessage::Pong(payload));
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Err(err)) => return Err(anyhow::anyhow!(err)),
                    }
                }
                _ = sleep(idle_timeout) => {
                    let _ = reader_tx.try_send(OutboundMessage::Close {
                        code: axum::extract::ws::close_code::NORMAL,
                        reason: "idle timeout",
                    });
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    tokio::pin!(reader);
    tokio::pin!(writer);
    tokio::pin!(fanout);

    let (completed_task, result) = tokio::select! {
        result = &mut reader => (ConnectionTask::Reader, task_result("reader", result)),
        result = &mut writer => (ConnectionTask::Writer, task_result("writer", result)),
        result = &mut fanout => (ConnectionTask::Fanout, task_result("fanout", result)),
    };

    match completed_task {
        ConnectionTask::Reader => {
            fanout.abort();
            writer.abort();
            let _ = fanout.await;
            let _ = writer.await;
        }
        ConnectionTask::Writer => {
            reader.abort();
            fanout.abort();
            let _ = reader.await;
            let _ = fanout.await;
        }
        ConnectionTask::Fanout => {
            reader.abort();
            writer.abort();
            let _ = reader.await;
            let _ = writer.await;
        }
    }

    state.unregister_client(&client_id, &tenant_id, connection_id);
    result
}

async fn handle_text(
    state: &SharedState,
    client_tx: &mpsc::Sender<OutboundMessage>,
    default_topic_key: &str,
    tenant_id: &str,
    client_id: &str,
    text: &str,
) {
    match parse_client_event_text(text) {
        Ok(event) => {
            handle_client_event(
                state,
                client_tx,
                default_topic_key,
                tenant_id,
                client_id,
                event,
            )
            .await
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

async fn handle_binary(
    state: &SharedState,
    client_tx: &mpsc::Sender<OutboundMessage>,
    default_topic_key: &str,
    tenant_id: &str,
    client_id: &str,
    raw: &[u8],
) {
    match parse_client_event_binary(raw) {
        Ok(event) => {
            handle_client_event(
                state,
                client_tx,
                default_topic_key,
                tenant_id,
                client_id,
                event,
            )
            .await
        }
        Err(_) => {
            state.metrics.protocol_error();
            send_error(
                client_tx,
                "invalid_msgpack",
                "binary message must match the websocket MessagePack protocol",
                state,
            );
        }
    }
}

async fn handle_client_event(
    state: &SharedState,
    client_tx: &mpsc::Sender<OutboundMessage>,
    default_topic_key: &str,
    tenant_id: &str,
    client_id: &str,
    event: ClientEvent,
) {
    match event {
        ClientEvent::Publish {
            topic,
            request_id,
            payload,
        } => {
            let topic_key = topic
                .as_deref()
                .map(|t| tenant_topic(tenant_id, &normalize_topic(t)))
                .unwrap_or_else(|| default_topic_key.to_owned());
            match encode_server_event(&ServerEvent::Message {
                topic: extract_topic(&topic_key),
                from: client_id,
                request_id: request_id.as_deref(),
                payload: &payload,
            }) {
                Ok(encoded) => {
                    let receivers = state.publish(&topic_key, encoded);
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
        ClientEvent::Direct {
            to,
            request_id,
            payload,
        } => match encode_server_event(&ServerEvent::DirectMessage {
            from: client_id,
            to: &to,
            request_id: request_id.as_deref(),
            payload: &payload,
        }) {
            Ok(encoded) => {
                if !state.send_to_client(&to, tenant_id, encoded) {
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
        ClientEvent::Ping {
            request_id,
            payload,
        } => {
            match encode_server_event(&ServerEvent::Pong {
                request_id: request_id.as_deref(),
                payload: payload.as_ref(),
            }) {
                Ok(encoded) => {
                    let _ = send_encoded(client_tx, encoded, state);
                }
                Err(_) => send_error(client_tx, "encode_failed", "failed to encode pong", state),
            }
        }
    }
}

fn send_error(
    client_tx: &mpsc::Sender<OutboundMessage>,
    code: &str,
    message: &str,
    state: &SharedState,
) {
    if let Ok(encoded) = encode_server_event(&ServerEvent::Error { code, message }) {
        let _ = send_encoded(client_tx, encoded, state);
    }
}

fn send_encoded(
    client_tx: &mpsc::Sender<OutboundMessage>,
    message: bytes::Bytes,
    state: &SharedState,
) -> bool {
    match client_tx.try_send(OutboundMessage::Binary(message)) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            state.metrics.message_dropped();
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            state.metrics.message_dropped();
            false
        }
    }
}

fn task_result(
    task_name: &str,
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Result<()> {
    match result {
        Ok(result) => result,
        Err(err) if err.is_cancelled() => Ok(()),
        Err(err) => Err(anyhow::anyhow!("{task_name} task failed: {err}")),
    }
}

/// Extract the client-facing topic name from an internal `tenant:topic` key.
/// Falls back to the whole string if no `:` is present (defensive).
fn extract_topic(topic_key: &str) -> &str {
    topic_key.split_once(':').map(|(_, t)| t).unwrap_or(topic_key)
}

#[derive(Clone, Copy, Debug)]
enum ConnectionTask {
    Reader,
    Writer,
    Fanout,
}

#[derive(Debug)]
struct RateLimiter {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(refill_per_second: u32, burst: u32) -> Self {
        let capacity = burst.max(1) as f64;
        Self {
            capacity,
            tokens: capacity,
            refill_per_second: refill_per_second.max(1) as f64,
            last_refill: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        self.last_refill = now;
        self.tokens =
            (self.tokens + tokens_to_add(elapsed, self.refill_per_second)).min(self.capacity);
    }
}

fn tokens_to_add(elapsed: Duration, refill_per_second: f64) -> f64 {
    elapsed.as_secs_f64() * refill_per_second
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
