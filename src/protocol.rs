use bytes::Bytes;
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

pub fn parse_client_event_text(raw: &str) -> serde_json::Result<ClientEvent> {
    serde_json::from_str(raw)
}

pub fn parse_client_event_binary(raw: &[u8]) -> Result<ClientEvent, rmp_serde::decode::Error> {
    rmp_serde::from_slice(raw)
}

pub fn encode_server_event(event: &ServerEvent<'_>) -> Result<Bytes, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(event).map(Bytes::from)
}
