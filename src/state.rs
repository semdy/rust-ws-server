use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::extract::ws::{CloseFrame, Message};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{Semaphore, broadcast, mpsc};

use crate::{
    auth::AuthVerifier,
    config::Config,
    ip_limiter::IpLimiter,
    metrics::Metrics,
};

pub type SharedState = Arc<AppState>;

#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    pub metrics: Metrics,
    pub connection_limit: Arc<Semaphore>,
    pub auth: Option<AuthVerifier>,
    pub ip_limiter: Option<Arc<IpLimiter>>,
    topics: DashMap<String, broadcast::Sender<Bytes>>,
    // Keyed by (tenant_id, client_id) so the same client_id can exist in different tenants.
    clients: DashMap<(String, String), ClientHandle>,
    next_connection_id: AtomicU64,
}

#[derive(Clone, Debug)]
struct ClientHandle {
    connection_id: u64,
    sender: mpsc::Sender<OutboundMessage>,
}

#[derive(Clone, Debug)]
pub enum OutboundMessage {
    Binary(Bytes),
    Pong(Vec<u8>),
    Close { code: u16, reason: &'static str },
}

impl OutboundMessage {
    pub fn into_ws_message(self) -> Message {
        match self {
            Self::Binary(bytes) => Message::Binary(bytes.to_vec()),
            Self::Pong(payload) => Message::Pong(payload),
            Self::Close { code, reason } => Message::Close(Some(CloseFrame {
                code,
                reason: reason.into(),
            })),
        }
    }
}

impl AppState {
    pub fn new(config: Config) -> SharedState {
        let auth = AuthVerifier::from_config(&config);
        let ip_limiter = IpLimiter::from_config(&config);
        Arc::new(Self {
            connection_limit: Arc::new(Semaphore::new(config.max_connections)),
            auth,
            ip_limiter,
            config,
            metrics: Metrics::default(),
            topics: DashMap::new(),
            clients: DashMap::new(),
            next_connection_id: AtomicU64::new(1),
        })
    }

    pub fn next_connection_id(&self) -> u64 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn register_client(
        &self,
        client_id: String,
        tenant_id: String,
        connection_id: u64,
        sender: mpsc::Sender<OutboundMessage>,
    ) {
        self.clients.insert(
            (tenant_id, client_id),
            ClientHandle {
                connection_id,
                sender,
            },
        );
    }

    pub fn unregister_client(&self, client_id: &str, tenant_id: &str, connection_id: u64) {
        let key = (tenant_id.to_owned(), client_id.to_owned());
        if self
            .clients
            .get(&key)
            .is_some_and(|handle| handle.connection_id == connection_id)
        {
            self.clients.remove(&key);
        }
    }

    /// Send a direct message to `client_id` within `tenant_id`.
    /// Cross-tenant sends are treated as "client not found" to avoid leaking existence.
    pub fn send_to_client(&self, client_id: &str, tenant_id: &str, message: Bytes) -> bool {
        let key = (tenant_id.to_owned(), client_id.to_owned());
        match self.clients.get(&key) {
            Some(handle) => handle
                .sender
                .try_send(OutboundMessage::Binary(message))
                .is_ok(),
            None => false,
        }
    }

    pub fn publish(&self, topic_key: &str, message: Bytes) -> usize {
        let sender = self.topic_sender(topic_key);
        sender.send(message).unwrap_or(0)
    }

    pub fn subscribe(&self, topic_key: &str) -> broadcast::Receiver<Bytes> {
        self.topic_sender(topic_key).subscribe()
    }

    fn topic_sender(&self, topic_key: &str) -> broadcast::Sender<Bytes> {
        self.topics
            .entry(topic_key.to_owned())
            .or_insert_with(|| broadcast::channel(self.config.topic_channel_capacity).0)
            .clone()
    }
}

/// Compose the internal topic storage key from tenant and client-facing topic name.
/// Tenants are isolated by prefixing the topic; clients never see this prefix.
pub fn tenant_topic(tenant_id: &str, topic: &str) -> String {
    format!("{tenant_id}:{topic}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_config() -> Config {
        Config {
            bind_addr: "0.0.0.0:0".parse::<SocketAddr>().unwrap(),
            max_connections: 10,
            client_queue_capacity: 16,
            topic_channel_capacity: 32,
            max_text_bytes: 64 * 1024,
            max_messages_per_second: 100,
            message_burst: 200,
            idle_timeout: std::time::Duration::from_secs(5),
            heartbeat_interval: std::time::Duration::from_secs(60),
            json_logs: false,
            jwt_secret: None,
            jwt_public_key: None,
            jwt_issuer: None,
            ip_max_concurrent: None,
            ip_connection_rate: None,
            ip_rate_burst: None,
            trust_proxy_headers: false,
        }
    }

    #[test]
    fn direct_message_is_scoped_to_tenant() {
        let state = AppState::new(test_config());
        let (tx_a, _rx_a) = mpsc::channel::<OutboundMessage>(16);
        let (tx_b, _rx_b) = mpsc::channel::<OutboundMessage>(16);
        // Same client_id "alice" registered in two different tenants — both must coexist.
        state.register_client("alice".into(), "t1".into(), 1, tx_a);
        state.register_client("alice".into(), "t2".into(), 2, tx_b);

        // Send to alice in t1 hits t1's handle; t2's queue stays empty.
        assert!(state.send_to_client("alice", "t1", Bytes::from_static(b"x")));
        // Send to alice in t2 hits t2's handle.
        assert!(state.send_to_client("alice", "t2", Bytes::from_static(b"y")));
    }

    #[test]
    fn cross_tenant_direct_returns_false() {
        let state = AppState::new(test_config());
        let (tx, _rx) = mpsc::channel::<OutboundMessage>(16);
        state.register_client("bob".into(), "t1".into(), 1, tx);

        // Bob exists in t1 but we ask for t2 — should be treated as not found.
        let payload = Bytes::from_static(b"y");
        assert!(!state.send_to_client("bob", "t2", payload));
    }

    #[test]
    fn tenant_topic_prefixes_namespace() {
        assert_eq!(tenant_topic("t1", "room-a"), "t1:room-a");
        assert_eq!(tenant_topic("default", "room-a"), "default:room-a");
        assert_ne!(tenant_topic("t1", "room-a"), tenant_topic("t2", "room-a"));
    }
}
