use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    active_connections: AtomicU64,
    accepted_connections: AtomicU64,
    rejected_connections: AtomicU64,
    messages_in: AtomicU64,
    messages_out: AtomicU64,
    dropped_messages: AtomicU64,
    protocol_errors: AtomicU64,
}

impl Metrics {
    pub fn connection_accepted(&self) {
        self.accepted_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn connection_rejected(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn message_in(&self) {
        self.messages_in.fetch_add(1, Ordering::Relaxed);
    }

    pub fn message_out(&self) {
        self.messages_out.fetch_add(1, Ordering::Relaxed);
    }

    pub fn message_dropped(&self) {
        self.dropped_messages.fetch_add(1, Ordering::Relaxed);
    }

    pub fn protocol_error(&self) {
        self.protocol_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        format!(
            concat!(
                "# HELP ws_active_connections Active websocket connections.\n",
                "# TYPE ws_active_connections gauge\n",
                "ws_active_connections {}\n",
                "# HELP ws_accepted_connections_total Accepted websocket connections.\n",
                "# TYPE ws_accepted_connections_total counter\n",
                "ws_accepted_connections_total {}\n",
                "# HELP ws_rejected_connections_total Rejected websocket connections.\n",
                "# TYPE ws_rejected_connections_total counter\n",
                "ws_rejected_connections_total {}\n",
                "# HELP ws_messages_in_total Inbound websocket messages.\n",
                "# TYPE ws_messages_in_total counter\n",
                "ws_messages_in_total {}\n",
                "# HELP ws_messages_out_total Outbound websocket messages.\n",
                "# TYPE ws_messages_out_total counter\n",
                "ws_messages_out_total {}\n",
                "# HELP ws_dropped_messages_total Dropped websocket messages.\n",
                "# TYPE ws_dropped_messages_total counter\n",
                "ws_dropped_messages_total {}\n",
                "# HELP ws_protocol_errors_total Invalid websocket protocol messages.\n",
                "# TYPE ws_protocol_errors_total counter\n",
                "ws_protocol_errors_total {}\n",
            ),
            self.active_connections.load(Ordering::Relaxed),
            self.accepted_connections.load(Ordering::Relaxed),
            self.rejected_connections.load(Ordering::Relaxed),
            self.messages_in.load(Ordering::Relaxed),
            self.messages_out.load(Ordering::Relaxed),
            self.dropped_messages.load(Ordering::Relaxed),
            self.protocol_errors.load(Ordering::Relaxed),
        )
    }
}
