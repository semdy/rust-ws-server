use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::extract::ws::{CloseFrame, Message};
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{Semaphore, broadcast, mpsc};

use crate::{config::Config, metrics::Metrics};

pub type SharedState = Arc<AppState>;

#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    pub metrics: Metrics,
    pub connection_limit: Arc<Semaphore>,
    topics: DashMap<String, broadcast::Sender<Bytes>>,
    clients: DashMap<String, ClientHandle>,
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
        Arc::new(Self {
            connection_limit: Arc::new(Semaphore::new(config.max_connections)),
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
        connection_id: u64,
        sender: mpsc::Sender<OutboundMessage>,
    ) {
        self.clients.insert(
            client_id,
            ClientHandle {
                connection_id,
                sender,
            },
        );
    }

    pub fn unregister_client(&self, client_id: &str, connection_id: u64) {
        if self
            .clients
            .get(client_id)
            .is_some_and(|handle| handle.connection_id == connection_id)
        {
            self.clients.remove(client_id);
        }
    }

    pub fn send_to_client(&self, client_id: &str, message: Bytes) -> bool {
        let sender = self
            .clients
            .get(client_id)
            .map(|handle| handle.sender.clone());

        match sender {
            Some(sender) => sender.try_send(OutboundMessage::Binary(message)).is_ok(),
            None => false,
        }
    }

    pub fn publish(&self, topic: &str, message: Bytes) -> usize {
        let sender = self.topic_sender(topic);
        sender.send(message).unwrap_or(0)
    }

    pub fn subscribe(&self, topic: &str) -> broadcast::Receiver<Bytes> {
        self.topic_sender(topic).subscribe()
    }

    fn topic_sender(&self, topic: &str) -> broadcast::Sender<Bytes> {
        self.topics
            .entry(topic.to_owned())
            .or_insert_with(|| broadcast::channel(self.config.topic_channel_capacity).0)
            .clone()
    }
}
