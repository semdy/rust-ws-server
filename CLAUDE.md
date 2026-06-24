# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
cargo test                    # all tests (unit + integration)
cargo test --lib              # unit tests only (within src/)
cargo test --test websocket   # integration tests only (tests/websocket.rs)
cargo test api_               # filter by test name prefix
cargo build --release         # release binary
```

## Architecture

This is a single-binary WebSocket message broker built on Tokio + Axum. It is not a multi-instance distributed system — broadcast is in-process via `tokio::sync::broadcast` channels inside a single `AppState`.

**Crate structure**: both `main.rs` and `lib.rs` declare `mod` for each source file (the modules are compiled into both the binary and library crates). Integration tests use `rust_ws_server::*` from the library side. Changing a `pub(crate)` item requires checking both compilation paths.

### Request flow

1. `main.rs` loads `.env` via `dotenvy`, parses `Config` via clap (all fields env-driven), calls `server::serve`.
2. `server::serve` builds an Axum `Router` with `into_make_service_with_connect_info::<SocketAddr>()` to enable `ConnectInfo` extraction.
3. `ws_handler` (the `/ws` upgrade endpoint) runs admission checks in order:
   - Resolve real client IP (`X-Forwarded-For` / `X-Real-IP` if `trust_proxy_headers`, else socket peer)
   - IP limiter (`IpLimiter`): concurrent cap + token-bucket rate, returns 429 on rejection
   - Global connection semaphore (`max_connections`), returns 503
   - JWT verification (if `AuthVerifier` is configured from `jwt_secret` or `jwt_public_key`), returns 401
   - Per-tenant connection semaphore (`tenant_max_connections`), returns 429
4. On upgrade, `handle_socket` spawns three async tasks per connection:
   - **Writer**: drains the per-client `mpsc` queue, sends to the WS socket, fires heartbeats (`Ping`)
   - **Fanout**: reads from the topic `broadcast::Receiver`, pushes to the per-client queue
   - **Reader**: reads WS frames, applies per-connection rate limit → per-tenant rate limit → dispatches to `handle_client_event`
5. The three tasks race via `tokio::select!`; whichever finishes first aborts the other two.

### Module map

| File | Purpose |
|------|---------|
| `config.rs` | `Config` struct via clap `#[derive(Parser)]`. Every field has `#[arg(long, env = "...")]`. All optional features default off. |
| `state.rs` | `AppState` — the single `Arc`-wrapped server state. Holds `DashMap` for clients (keyed by `(tenant_id, client_id)`) and topics, per-tenant semaphores and rate buckets, plus the global connection semaphore and `Metrics`. |
| `server.rs` | Router definition, WS upgrade handler, connection lifecycle (reader/writer/fanout tasks), per-connection `RateLimiter`, shutdown signal. |
| `protocol.rs` | `ClientEvent` (deserialize, `#[serde(tag = "kind")]`) and `ServerEvent` (serialize). Parsers for JSON text and MessagePack binary. Outbound always MessagePack binary via `rmp_serde::to_vec_named`. |
| `auth.rs` | `AuthVerifier` — JWT decode + validation. `from_config()` returns `None` when no secret/key is configured (auth disabled). `verify()` returns `AuthIdentity { client_id, tenant_id }`. |
| `ip_limiter.rs` | `IpLimiter` — DashMap of `IpState` per IP, concurrent cap + token bucket. `IpPermit` releases on Drop. Idle entries reaped lazily. |
| `metrics.rs` | 11 `AtomicU64` counters rendered as Prometheus text format via `render_prometheus()`. |
| `api.rs` | HTTP API handlers: `POST /api/publish` and `POST /api/direct`. Use Bearer token auth (same JWT as WS). |

### Key design decisions

- **Topic keys are internal**: `tenant_topic(tenant, topic)` produces `"t1:room-a"`. Clients never see or send this prefix — it's stripped from `Ready` and `Message` events via `extract_topic()`.
- **Per-tenant rate limit drops messages, does not close connections** (`continue` vs `break` in the reader task). The per-connection rate limit does close the socket.
- **Client registration uses `(tenant_id, client_id)` composite key** so the same `client_id` can coexist in different tenants. Cross-tenant direct messages return `client_not_found` without leaking the target's tenant.
- **Connection ID + compare-and-swap unregistration**: `unregister_client` checks `connection_id` before removing the entry, so a reconnected client with the same key doesn't get its predecessor's stale removal clobbering it.
- **`main.rs` + `lib.rs` dual `mod`**: both crate roots declare the same modules. Removing `mod` from `main.rs` would require changing `crate::` paths there to `rust_ws_server::`, but other files are unaffected since their `crate::` resolves within the crate they're compiled into.
