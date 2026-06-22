use std::{net::SocketAddr, time::Duration};

use clap::Parser;

#[derive(Clone, Debug, Parser)]
#[command(author, version, about)]
pub struct Config {
    #[arg(long, env = "WS_BIND_ADDR", default_value = "0.0.0.0:8080")]
    pub bind_addr: SocketAddr,

    #[arg(long, env = "WS_MAX_CONNECTIONS", default_value_t = 10_000)]
    pub max_connections: usize,

    #[arg(long, env = "WS_CLIENT_QUEUE_CAPACITY", default_value_t = 256)]
    pub client_queue_capacity: usize,

    #[arg(long, env = "WS_TOPIC_CHANNEL_CAPACITY", default_value_t = 1024)]
    pub topic_channel_capacity: usize,

    #[arg(long, env = "WS_MAX_TEXT_BYTES", default_value_t = 64 * 1024)]
    pub max_text_bytes: usize,

    #[arg(long, env = "WS_MAX_MESSAGES_PER_SECOND", default_value_t = 100)]
    pub max_messages_per_second: u32,

    #[arg(long, env = "WS_MESSAGE_BURST", default_value_t = 200)]
    pub message_burst: u32,

    #[arg(long, env = "WS_IDLE_TIMEOUT", default_value = "60s", value_parser = humantime::parse_duration)]
    pub idle_timeout: Duration,

    #[arg(long, env = "WS_HEARTBEAT_INTERVAL", default_value = "20s", value_parser = humantime::parse_duration)]
    pub heartbeat_interval: Duration,

    #[arg(long, env = "WS_JSON_LOGS", default_value_t = false)]
    pub json_logs: bool,

    /// HMAC secret for JWT verification (HS256). When set, enables JWT auth.
    #[arg(long, env = "WS_JWT_SECRET")]
    pub jwt_secret: Option<String>,

    /// PEM-encoded public key for JWT verification (RS256/EdDSA). Alternative to HS256 secret.
    #[arg(long, env = "WS_JWT_PUBLIC_KEY")]
    pub jwt_public_key: Option<String>,

    /// Optional expected `iss` claim; if set, tokens with mismatched issuer are rejected.
    #[arg(long, env = "WS_JWT_ISSUER")]
    pub jwt_issuer: Option<String>,

    /// Max concurrent websocket connections per client IP. None = unlimited.
    #[arg(long, env = "WS_IP_MAX_CONCURRENT")]
    pub ip_max_concurrent: Option<usize>,

    /// Per-IP new-connection rate limit (connections per second). None = unlimited.
    #[arg(long, env = "WS_IP_CONNECTION_RATE")]
    pub ip_connection_rate: Option<u32>,

    /// Burst size for per-IP connection rate limiter. Defaults to the rate when unset.
    #[arg(long, env = "WS_IP_RATE_BURST")]
    pub ip_rate_burst: Option<u32>,

    /// Trust X-Forwarded-For / X-Real-IP headers for client IP. Only enable behind a trusted reverse proxy.
    #[arg(long, env = "WS_TRUST_PROXY_HEADERS", default_value_t = false)]
    pub trust_proxy_headers: bool,

    /// Max concurrent websocket connections per tenant (identified by JWT `tenant_id`).
    /// None = unlimited. Prevents one noisy tenant from starving others on a shared instance.
    #[arg(long, env = "WS_TENANT_MAX_CONNECTIONS")]
    pub tenant_max_connections: Option<usize>,

    /// Per-tenant inbound message rate (messages per second, aggregated across all of the
    /// tenant's connections). None = unlimited. Caps a noisy tenant's publish/direct volume
    /// so it cannot saturate the broadcast queues of other tenants' topics.
    #[arg(long, env = "WS_TENANT_MAX_MESSAGES_PER_SECOND")]
    pub tenant_max_messages_per_second: Option<u32>,

    /// Burst size for the per-tenant message rate limiter. Defaults to the rate when unset.
    #[arg(long, env = "WS_TENANT_MESSAGE_BURST")]
    pub tenant_message_burst: Option<u32>,
}
