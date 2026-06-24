use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::SocketAddr;

use crate::{
    auth::AuthIdentity,
    protocol::{ServerEvent, encode_server_event},
    state::{SharedState, tenant_topic},
};

#[derive(Debug, Deserialize)]
pub(crate) struct PublishBody {
    topic: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    payload: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct PublishResponse {
    receivers: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DirectBody {
    to: String,
    #[serde(default)]
    request_id: Option<String>,
    payload: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct DirectResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct ErrorResponse {
    error: String,
}

/// Extract `Bearer <token>` from the Authorization header.
fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Resolve an `AuthIdentity` from the request. Follows the same logic as `ws_handler`:
/// if auth is configured, require a valid Bearer token; otherwise use a default identity.
fn resolve_identity(state: &SharedState, headers: &HeaderMap) -> Result<AuthIdentity, (StatusCode, String)> {
    match &state.auth {
        Some(verifier) => {
            let token = extract_bearer(headers)
                .ok_or_else(|| (StatusCode::UNAUTHORIZED, "missing Authorization header\n".into()))?;
            verifier.verify(token).map_err(|err| {
                (StatusCode::UNAUTHORIZED, format!("invalid token: {err}\n"))
            })
        }
        None => Ok(AuthIdentity {
            client_id: "api".into(),
            tenant_id: "default".into(),
        }),
    }
}

pub(crate) async fn api_publish(
    State(state): State<SharedState>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<PublishBody>,
) -> Response {
    let identity = match resolve_identity(&state, &headers) {
        Ok(id) => id,
        Err((status, msg)) => return (status, Json(ErrorResponse { error: msg })).into_response(),
    };

    let topic = body.topic.as_deref().unwrap_or("public");
    let topic_key = tenant_topic(&identity.tenant_id, &crate::server::normalize_topic(topic));

    match encode_server_event(&ServerEvent::Message {
        topic: &topic,
        from: &identity.client_id,
        request_id: body.request_id.as_deref(),
        payload: &body.payload,
    }) {
        Ok(encoded) => {
            let receivers = state.publish(&topic_key, encoded);
            if receivers == 0 {
                state.metrics.message_dropped();
            } else {
                // One outbound message per receiver for metrics parity.
                for _ in 0..receivers {
                    state.metrics.message_out();
                }
            }
            (StatusCode::OK, Json(PublishResponse { receivers })).into_response()
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "encode failed".into(),
            }),
        ).into_response(),
    }
}

pub(crate) async fn api_direct(
    State(state): State<SharedState>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<DirectBody>,
) -> Response {
    let identity = match resolve_identity(&state, &headers) {
        Ok(id) => id,
        Err((status, msg)) => return (status, Json(ErrorResponse { error: msg })).into_response(),
    };

    match encode_server_event(&ServerEvent::DirectMessage {
        from: &identity.client_id,
        to: &body.to,
        request_id: body.request_id.as_deref(),
        payload: &body.payload,
    }) {
        Ok(encoded) => {
            if state.send_to_client(&body.to, &identity.tenant_id, encoded) {
                state.metrics.message_out();
                (StatusCode::OK, Json(DirectResponse { ok: true, error: None })).into_response()
            } else {
                state.metrics.message_dropped();
                (
                    StatusCode::NOT_FOUND,
                    Json(DirectResponse {
                        ok: false,
                        error: Some("client_not_found".into()),
                    }),
                )
                    .into_response()
            }
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DirectResponse {
                ok: false,
                error: Some("encode failed".into()),
            }),
        )
            .into_response(),
    }
}
