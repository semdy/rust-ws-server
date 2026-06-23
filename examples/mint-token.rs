//! Mint a JWT for connecting to the websocket server.
//!
//! Reads `WS_JWT_SECRET` (must match the server's configured secret) and emits a signed
//! HS256 token to stdout. The resulting token goes into the `?token=...` query param.
//!
//! Usage:
//!     WS_JWT_SECRET=mysecret CLIENT_ID=alice TENANT_ID=t1 cargo run --example mint-token
//!
//! Environment variables:
//!     WS_JWT_SECRET  (required) HMAC secret, must match the server.
//!     CLIENT_ID      (default "alice") Becomes the JWT `sub` and the trusted client_id.
//!     TENANT_ID      (optional) Becomes the JWT `tenant_id`. Omit to land in `default`.
//!     TTL_SECS       (default 3600) Token validity in seconds from now.
//!     ISS            (optional) Issuer claim; only needed if the server sets WS_JWT_ISSUER.

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct Claims {
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    exp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
}

fn now_plus(secs: i64) -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs() as i64
        + secs
}

fn main() {
    let secret = std::env::var("WS_JWT_SECRET").expect("WS_JWT_SECRET must be set");
    let client_id = std::env::var("CLIENT_ID").unwrap_or_else(|_| "alice".to_owned());
    let tenant_id = std::env::var("TENANT_ID").ok();
    let iss = std::env::var("ISS").ok();
    let ttl: i64 = std::env::var("TTL_SECS")
        .ok()
        .map(|s| s.parse().expect("TTL_SECS must be an integer"))
        .unwrap_or(3600);

    let claims = Claims {
        sub: client_id,
        tenant_id,
        exp: now_plus(ttl),
        iss,
    };

    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("failed to encode jwt");

    println!("{token}");
}
