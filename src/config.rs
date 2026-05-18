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

    #[arg(long, env = "WS_IDLE_TIMEOUT", default_value = "60s", value_parser = humantime::parse_duration)]
    pub idle_timeout: Duration,

    #[arg(long, env = "WS_HEARTBEAT_INTERVAL", default_value = "20s", value_parser = humantime::parse_duration)]
    pub heartbeat_interval: Duration,

    #[arg(long, env = "WS_JSON_LOGS", default_value_t = false)]
    pub json_logs: bool,
}
