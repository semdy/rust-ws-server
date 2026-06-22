use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::config::Config;

#[derive(Debug)]
struct IpState {
    /// Current concurrent connections for this IP.
    concurrent: u32,
    /// Token bucket for connection rate. Only enforced when `rate_configured` is true.
    bucket: Mutex<TokenBucket>,
    last_seen: Instant,
}

#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(refill_per_second: f64, burst: f64) -> Self {
        let capacity = burst.max(1.0);
        Self {
            capacity,
            tokens: capacity,
            refill_per_second: refill_per_second.max(1.0),
            last_refill: Instant::now(),
        }
    }

    fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        self.last_refill = now;
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.refill_per_second).min(self.capacity);
    }
}

const IDLE_TTL: Duration = Duration::from_secs(5 * 60);

/// IP-scoped connection limiter. Enforces both a concurrent-connection cap and a
/// new-connection rate per source IP. Entries idle for `IDLE_TTL` with zero concurrency
/// are lazily reaped on access to bound memory.
#[derive(Debug)]
pub struct IpLimiter {
    max_concurrent: Option<u32>,
    rate_configured: bool,
    refill_per_second: f64,
    burst: f64,
    states: DashMap<IpAddr, IpState>,
}

impl IpLimiter {
    pub fn from_config(config: &Config) -> Option<Arc<Self>> {
        let max_concurrent = config.ip_max_concurrent.map(|v| v as u32);
        let rate = config.ip_connection_rate.map(|v| v as f64);
        let burst = config
            .ip_rate_burst
            .map(|v| v as f64)
            .or_else(|| rate.map(|r| r.max(1.0)));

        if max_concurrent.is_none() && rate.is_none() {
            return None;
        }

        let rate_configured = rate.is_some();
        let refill_per_second = rate.unwrap_or(1.0);
        let burst = burst.unwrap_or(1.0);

        Some(Arc::new(Self {
            max_concurrent,
            rate_configured,
            refill_per_second,
            burst,
            states: DashMap::new(),
        }))
    }

    /// Attempt to admit a new connection from `ip`. On success returns a permit that
    /// decrements the concurrent counter on drop. On failure returns the rejection reason.
    pub fn try_acquire(self: &Arc<Self>, ip: IpAddr) -> Result<IpPermit, IpRejection> {
        self.reap_idle();

        let entry = self
            .states
            .entry(ip)
            .or_insert_with(|| IpState {
                concurrent: 0,
                bucket: Mutex::new(TokenBucket::new(self.refill_per_second, self.burst)),
                last_seen: Instant::now(),
            });

        let mut state = entry;

        // Rate check first (no state mutation on rejection except bucket token consumption).
        if self.rate_configured {
            let mut bucket = state.bucket.lock();
            if !bucket.try_consume() {
                return Err(IpRejection::RateLimited);
            }
        }

        // Concurrent cap.
        if let Some(cap) = self.max_concurrent
            && state.concurrent >= cap
        {
            return Err(IpRejection::ConcurrencyLimited);
        }

        state.concurrent += 1;
        state.last_seen = Instant::now();
        let limiter = Arc::clone(self);
        Ok(IpPermit {
            ip: Some(ip),
            limiter,
        })
    }

    fn reap_idle(&self) {
        let now = Instant::now();
        let mut victims: Vec<IpAddr> = Vec::new();
        for entry in self.states.iter() {
            if entry.concurrent == 0 && now.duration_since(entry.last_seen) > IDLE_TTL {
                victims.push(*entry.key());
                if victims.len() >= 64 {
                    break;
                }
            }
        }
        for ip in victims {
            self.states.remove_if(&ip, |_, state| {
                state.concurrent == 0 && now.duration_since(state.last_seen) > IDLE_TTL
            });
        }
    }

    fn release(&self, ip: IpAddr) {
        if let Some(mut state) = self.states.get_mut(&ip) {
            if state.concurrent > 0 {
                state.concurrent -= 1;
            }
            state.last_seen = Instant::now();
        }
    }
}

/// Permit returned by [`IpLimiter::try_acquire`]. Releases the concurrent slot on drop.
#[derive(Debug)]
pub struct IpPermit {
    ip: Option<IpAddr>,
    limiter: Arc<IpLimiter>,
}

impl Drop for IpPermit {
    fn drop(&mut self) {
        if let Some(ip) = self.ip.take() {
            self.limiter.release(ip);
        }
    }
}

#[derive(Debug)]
pub enum IpRejection {
    /// New-connection rate exceeded for this IP.
    RateLimited,
    /// Concurrent-connection cap reached for this IP.
    ConcurrencyLimited,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::net::{Ipv4Addr, SocketAddr};

    fn config(max_concurrent: Option<usize>, rate: Option<u32>, burst: Option<u32>) -> Config {
        Config {
            bind_addr: "0.0.0.0:8080".parse::<SocketAddr>().unwrap(),
            max_connections: 100,
            client_queue_capacity: 16,
            topic_channel_capacity: 32,
            max_text_bytes: 64 * 1024,
            max_messages_per_second: 100,
            message_burst: 200,
            idle_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(60),
            json_logs: false,
            jwt_secret: None,
            jwt_public_key: None,
            jwt_issuer: None,
            ip_max_concurrent: max_concurrent,
            ip_connection_rate: rate,
            ip_rate_burst: burst,
            trust_proxy_headers: false,
        }
    }

    #[test]
    fn enforces_concurrency_cap() {
        let limiter = IpLimiter::from_config(&config(Some(1), None, None)).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let _p1 = limiter.try_acquire(ip).unwrap();
        match limiter.try_acquire(ip) {
            Err(IpRejection::ConcurrencyLimited) => {}
            other => panic!("expected ConcurrencyLimited, got {other:?}"),
        }
    }

    #[test]
    fn permit_release_allows_reentry() {
        let limiter = IpLimiter::from_config(&config(Some(1), None, None)).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        {
            let _p = limiter.try_acquire(ip).unwrap();
        }
        let _p2 = limiter.try_acquire(ip).unwrap();
    }

    #[test]
    fn enforces_rate_cap() {
        let limiter = IpLimiter::from_config(&config(None, Some(1), Some(1))).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let _first = limiter.try_acquire(ip).unwrap();
        match limiter.try_acquire(ip) {
            Err(IpRejection::RateLimited) => {}
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn none_when_unconfigured() {
        assert!(IpLimiter::from_config(&config(None, None, None)).is_none());
    }

    #[test]
    fn independent_ips_are_independent() {
        let limiter = IpLimiter::from_config(&config(Some(1), None, None)).unwrap();
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let _p1 = limiter.try_acquire(ip1).unwrap();
        let _p2 = limiter.try_acquire(ip2).unwrap();
    }

    #[test]
    fn rate_refills_over_time() {
        let limiter = IpLimiter::from_config(&config(None, Some(100), Some(1))).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let _first = limiter.try_acquire(ip).unwrap();
        // Bucket exhausted (capacity 1). Wait for refill.
        std::thread::sleep(Duration::from_millis(50));
        let _second = limiter.try_acquire(ip).unwrap();
    }
}
