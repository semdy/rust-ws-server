use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, Semaphore, broadcast};

use crate::{config::Config, metrics::Metrics};

pub type SharedState = Arc<AppState>;

#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    pub metrics: Metrics,
    pub connection_limit: Arc<Semaphore>,
    topics: RwLock<HashMap<String, broadcast::Sender<Arc<str>>>>,
}

impl AppState {
    pub fn new(config: Config) -> SharedState {
        Arc::new(Self {
            connection_limit: Arc::new(Semaphore::new(config.max_connections)),
            config,
            metrics: Metrics::default(),
            topics: RwLock::new(HashMap::new()),
        })
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
