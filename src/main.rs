mod config;
mod metrics;
mod protocol;
mod server;
mod state;

use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

use crate::{config::Config, state::AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    init_tracing(config.json_logs);
    server::serve(AppState::new(config)).await
}

fn init_tracing(json_logs: bool) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("rust_ws_server=info,tower_http=info"));

    if json_logs {
        fmt().json().with_env_filter(filter).init();
    } else {
        fmt().compact().with_env_filter(filter).init();
    }
}
