use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode,
    errors::ErrorKind,
};
use serde::Deserialize;

use crate::config::Config;

/// JWT claims used for websocket auth. `sub` becomes the trusted `client_id`,
/// `tenant_id` (defaulting to `"default"`) scopes topic namespace and direct messages.
#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub tenant_id: Option<String>,
    // `exp` and `iss` are validated by `jsonwebtoken`'s `Validation` during `decode`,
    // not read directly by us — hence the allow.
    #[allow(dead_code)]
    pub exp: i64,
    #[allow(dead_code)]
    pub iss: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AuthIdentity {
    pub client_id: String,
    pub tenant_id: String,
}

#[derive(Clone)]
enum Decoding {
    Hmac(DecodingKey),
    Asymmetric(DecodingKey, Algorithm),
}

#[derive(Clone)]
pub struct AuthVerifier {
    decoding: Decoding,
    issuer: Option<String>,
}

impl std::fmt::Debug for AuthVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthVerifier")
            .field("algorithm", &self.algorithm_name())
            .field("issuer", &self.issuer)
            .finish()
    }
}

impl AuthVerifier {
    fn algorithm_name(&self) -> &'static str {
        match &self.decoding {
            Decoding::Hmac(_) => "HS256",
            Decoding::Asymmetric(_, Algorithm::EdDSA) => "EdDSA",
            Decoding::Asymmetric(_, _) => "RS256",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("invalid token")]
    Invalid,
    #[error("token expired")]
    Expired,
    #[error("token issuer mismatch")]
    IssuerMismatch,
}

impl AuthVerifier {
    /// Build from config. Returns `None` if neither `jwt_secret` nor `jwt_public_key` is set,
    /// meaning auth is disabled.
    pub fn from_config(config: &Config) -> Option<Self> {
        if let Some(secret) = &config.jwt_secret {
            return Some(Self {
                decoding: Decoding::Hmac(DecodingKey::from_secret(secret.as_bytes())),
                issuer: config.jwt_issuer.clone(),
            });
        }
        if let Some(pem) = &config.jwt_public_key {
            // Prefer EdDSA if the PEM hints at an Ed25519 key, otherwise RS256.
            let algorithm = if pem.contains("ED25519") || pem.contains("Ed25519") {
                Algorithm::EdDSA
            } else {
                Algorithm::RS256
            };
            let key = DecodingKey::from_rsa_pem(pem.as_bytes())
                .or_else(|_| DecodingKey::from_ec_pem(pem.as_bytes()))
                .or_else(|_| DecodingKey::from_ed_pem(pem.as_bytes()))
                .map_err(|_| ())
                .ok()?;
            return Some(Self {
                decoding: Decoding::Asymmetric(key, algorithm),
                issuer: config.jwt_issuer.clone(),
            });
        }
        None
    }

    pub fn verify(&self, token: &str) -> Result<AuthIdentity, AuthError> {
        let mut validation = match &self.decoding {
            Decoding::Hmac(_) => Validation::new(Algorithm::HS256),
            Decoding::Asymmetric(_, algo) => Validation::new(*algo),
        };
        if let Some(iss) = &self.issuer {
            validation.set_issuer(&[iss]);
        }
        validation.validate_exp = true;

        let data = match decode::<Claims>(token, self.decoding_key(), &validation) {
            Ok(data) => data,
            Err(err) => {
                return match err.kind() {
                    ErrorKind::ExpiredSignature => Err(AuthError::Expired),
                    ErrorKind::InvalidIssuer => Err(AuthError::IssuerMismatch),
                    _ => Err(AuthError::Invalid),
                };
            }
        };

        let claims = data.claims;
        Ok(AuthIdentity {
            client_id: claims.sub,
            tenant_id: claims.tenant_id.unwrap_or_else(|| "default".to_owned()),
        })
    }

    fn decoding_key(&self) -> &DecodingKey {
        match &self.decoding {
            Decoding::Hmac(k) => k,
            Decoding::Asymmetric(k, _) => k,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_plus(secs: i64) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + secs
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        tenant_id: Option<&'a str>,
        exp: i64,
        iss: Option<&'a str>,
    }

    fn sign(claims: &TestClaims<'_>, secret: &str) -> String {
        encode(&Header::new(Algorithm::HS256), claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap()
    }

    fn verifier(secret: &str) -> AuthVerifier {
        let config = Config {
            jwt_secret: Some(secret.to_owned()),
            jwt_public_key: None,
            jwt_issuer: None,
            ..test_config()
        };
        AuthVerifier::from_config(&config).unwrap()
    }

    fn test_config() -> Config {
        use std::net::SocketAddr;
        Config {
            bind_addr: "0.0.0.0:8080".parse::<SocketAddr>().unwrap(),
            max_connections: 1,
            client_queue_capacity: 1,
            topic_channel_capacity: 1,
            max_text_bytes: 1,
            max_messages_per_second: 1,
            message_burst: 1,
            idle_timeout: std::time::Duration::from_secs(1),
            heartbeat_interval: std::time::Duration::from_secs(1),
            json_logs: false,
            jwt_secret: None,
            jwt_public_key: None,
            jwt_issuer: None,
            ip_max_concurrent: None,
            ip_connection_rate: None,
            ip_rate_burst: None,
            trust_proxy_headers: false,
            tenant_max_connections: None,
            tenant_max_messages_per_second: None,
            tenant_message_burst: None,
        }
    }

    #[test]
    fn accepts_valid_token() {
        let v = verifier("s3cret");
        let claims = TestClaims { sub: "alice", tenant_id: Some("t1"), exp: now_plus(3600), iss: None };
        let token = sign(&claims, "s3cret");
        let id = v.verify(&token).unwrap();
        assert_eq!(id.client_id, "alice");
        assert_eq!(id.tenant_id, "t1");
    }

    #[test]
    fn defaults_tenant_when_missing() {
        let v = verifier("s3cret");
        let claims = TestClaims { sub: "bob", tenant_id: None, exp: now_plus(3600), iss: None };
        let token = sign(&claims, "s3cret");
        let id = v.verify(&token).unwrap();
        assert_eq!(id.tenant_id, "default");
    }

    #[test]
    fn rejects_expired_token() {
        let v = verifier("s3cret");
        let claims = TestClaims { sub: "alice", tenant_id: None, exp: now_plus(-3600), iss: None };
        let token = sign(&claims, "s3cret");
        assert!(matches!(v.verify(&token), Err(AuthError::Expired)));
    }

    #[test]
    fn rejects_tampered_token() {
        let v = verifier("s3cret");
        let claims = TestClaims { sub: "alice", tenant_id: None, exp: now_plus(3600), iss: None };
        let mut token = sign(&claims, "s3cret");
        // Flip the last character of the signature segment.
        let last = token.len() - 1;
        let c = token.as_bytes()[last];
        token.replace_range(last.., if c == b'a' { "b" } else { "a" });
        assert!(matches!(v.verify(&token), Err(AuthError::Invalid)));
    }

    #[test]
    fn rejects_wrong_secret() {
        let v = verifier("s3cret");
        let claims = TestClaims { sub: "alice", tenant_id: None, exp: now_plus(3600), iss: None };
        let token = sign(&claims, "other-secret");
        assert!(matches!(v.verify(&token), Err(AuthError::Invalid)));
    }

    #[test]
    fn rejects_issuer_mismatch() {
        let config = Config {
            jwt_secret: Some("s3cret".into()),
            jwt_issuer: Some("expected-iss".into()),
            ..test_config()
        };
        let v = AuthVerifier::from_config(&config).unwrap();
        let claims = TestClaims { sub: "alice", tenant_id: None, exp: now_plus(3600), iss: Some("wrong-iss") };
        let token = sign(&claims, "s3cret");
        assert!(matches!(v.verify(&token), Err(AuthError::IssuerMismatch)));
    }

    #[test]
    fn disabled_when_no_secret_configured() {
        let config = Config {
            jwt_secret: None,
            jwt_public_key: None,
            ..test_config()
        };
        assert!(AuthVerifier::from_config(&config).is_none());
    }
}
