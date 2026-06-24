use clap::Parser;
use tracing_subscriber::{EnvFilter, fmt};

use rust_ws_server::{config::Config, state::AppState, server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load `.env` if present. Missing file is fine — production runs without it.
    // Must happen before `Config::parse()` so clap's env-var bindings see the values.
    let _ = dotenvy::dotenv();
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
