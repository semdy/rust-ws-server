use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientEvent {
    Publish {
        topic: Option<String>,
        request_id: Option<String>,
        payload: Value,
    },
    Direct {
        to: String,
        request_id: Option<String>,
        payload: Value,
    },
    Ping {
        request_id: Option<String>,
        payload: Option<Value>,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerEvent<'a> {
    Ready {
        topic: &'a str,
        client_id: &'a str,
    },
    Message {
        topic: &'a str,
        from: &'a str,
        request_id: Option<&'a str>,
        payload: &'a Value,
    },
    DirectMessage {
        from: &'a str,
        to: &'a str,
        request_id: Option<&'a str>,
        payload: &'a Value,
    },
    Pong {
        request_id: Option<&'a str>,
        payload: Option<&'a Value>,
    },
    Error {
        code: &'a str,
        message: &'a str,
    },
}

pub fn parse_client_event(raw: &str) -> serde_json::Result<ClientEvent> {
    serde_json::from_str(raw)
}

pub fn encode_server_event(event: &ServerEvent<'_>) -> serde_json::Result<String> {
    serde_json::to_string(event)
}
