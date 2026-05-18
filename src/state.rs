use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::extract::ws::Message;
use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};

use crate::{config::Config, metrics::Metrics};

pub type SharedState = Arc<AppState>;

#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    pub metrics: Metrics,
    pub connection_limit: Arc<Semaphore>,
    topics: RwLock<HashMap<String, broadcast::Sender<Arc<str>>>>,
    clients: RwLock<HashMap<String, ClientHandle>>,
    next_connection_id: AtomicU64,
}

#[derive(Clone, Debug)]
struct ClientHandle {
    connection_id: u64,
    sender: mpsc::Sender<Message>,
}

impl AppState {
    pub fn new(config: Config) -> SharedState {
        Arc::new(Self {
            connection_limit: Arc::new(Semaphore::new(config.max_connections)),
            config,
            metrics: Metrics::default(),
            topics: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
            next_connection_id: AtomicU64::new(1),
        })
    }

    pub fn next_connection_id(&self) -> u64 {
        self.next_connection_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn register_client(
        &self,
        client_id: String,
        connection_id: u64,
        sender: mpsc::Sender<Message>,
    ) {
        self.clients.write().await.insert(
            client_id,
            ClientHandle {
                connection_id,
                sender,
            },
        );
    }

    pub async fn unregister_client(&self, client_id: &str, connection_id: u64) {
        let mut clients = self.clients.write().await;
        if clients
            .get(client_id)
            .is_some_and(|handle| handle.connection_id == connection_id)
        {
            clients.remove(client_id);
        }
    }

    pub async fn send_to_client(&self, client_id: &str, message: Arc<str>) -> bool {
        let sender = self
            .clients
            .read()
            .await
            .get(client_id)
            .map(|handle| handle.sender.clone());

        match sender {
            Some(sender) => sender.try_send(Message::Text(message.to_string())).is_ok(),
            None => false,
        }
    }

    pub async fn publish(&self, topic: &str, message: Arc<str>) -> usize {
        let sender = self.topic_sender(topic).await;
        sender.send(message).unwrap_or(0)
    }

    pub async fn subscribe(&self, topic: &str) -> broadcast::Receiver<Arc<str>> {
        self.topic_sender(topic).await.subscribe()
    }

    async fn topic_sender(&self, topic: &str) -> broadcast::Sender<Arc<str>> {
        if let Some(sender) = self.topics.read().await.get(topic).cloned() {
            return sender;
        }

        let mut topics = self.topics.write().await;
        topics
            .entry(topic.to_owned())
            .or_insert_with(|| broadcast::channel(self.config.topic_channel_capacity).0)
            .clone()
    }
}
