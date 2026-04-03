use crate::sync::net::bandwidth::NetworkQuality;
use crate::util::error::{HarDataError, Result};
use quinn::Endpoint;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::config::configure_client_for_quality;

struct SessionTicketEntry {
    ticket: Vec<u8>,
    created_at: Instant,
}

pub struct SessionTicketStore {
    tickets: RwLock<HashMap<String, SessionTicketEntry>>,
    max_age: Duration,
}

impl SessionTicketStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tickets: RwLock::new(HashMap::new()),
            max_age: Duration::from_secs(24 * 60 * 60),
        })
    }

    pub async fn get(&self, endpoint: &str) -> Option<Vec<u8>> {
        let tickets = self.tickets.read().await;
        if let Some(entry) = tickets.get(endpoint) {
            if entry.created_at.elapsed() < self.max_age {
                return Some(entry.ticket.clone());
            }
        }
        None
    }
}

impl Default for SessionTicketStore {
    fn default() -> Self {
        Self {
            tickets: RwLock::new(HashMap::new()),
            max_age: Duration::from_secs(24 * 60 * 60),
        }
    }
}

#[derive(Clone)]
pub struct QuicClient {
    pub(super) endpoint: Endpoint,
    pub(super) server_addr: String,
    pub(super) server_name: String,
    pub(super) ticket_store: Arc<SessionTicketStore>,
}

impl QuicClient {
    pub fn new(server_addr: String, server_name: String, ca_cert_path: String) -> Result<Self> {
        Self::new_with_quality(server_addr, server_name, ca_cert_path, NetworkQuality::Good)
    }

    pub fn new_with_quality(
        server_addr: String,
        server_name: String,
        ca_cert_path: String,
        quality: NetworkQuality,
    ) -> Result<Self> {
        info!(
            "Creating QUIC client for server: {} (server_name={}, quality={:?})",
            server_addr, server_name, quality
        );

        let addr_clean = server_addr
            .strip_prefix("quic://")
            .unwrap_or(&server_addr)
            .to_string();
        let Some(server_name) = normalize_server_name(&server_name) else {
            return Err(HarDataError::InvalidConfig(
                "QUIC server name cannot be empty".to_string(),
            ));
        };

        let _ = rustls::crypto::ring::default_provider().install_default();

        let client_config = configure_client_for_quality(quality, &ca_cert_path)?;

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            server_addr: addr_clean,
            server_name,
            ticket_store: SessionTicketStore::new(),
        })
    }

    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    pub async fn connect(&self) -> Result<quinn::Connection> {
        if let Some(_ticket) = self.ticket_store.get(&self.server_addr).await {
            debug!("Attempting 0-RTT connection with cached ticket");
        }

        self.connect_standard().await
    }

    async fn connect_standard(&self) -> Result<quinn::Connection> {
        debug!("Connecting: {}", self.server_addr);

        use tokio::net::lookup_host;
        let addrs: Vec<_> = lookup_host(&self.server_addr)
            .await
            .map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Failed to resolve server address '{}': {}",
                    self.server_addr, e
                ))
            })?
            .collect();

        if addrs.is_empty() {
            return Err(HarDataError::InvalidConfig(format!(
                "No addresses found for server: {}",
                self.server_addr
            )));
        }

        let addr = addrs
            .iter()
            .find(|a| a.is_ipv4())
            .or_else(|| addrs.first())
            .copied()
            .ok_or_else(|| {
                HarDataError::InvalidConfig(format!(
                    "Failed to select address for server: {}",
                    self.server_addr
                ))
            })?;

        debug!("Resolved {} to {}", self.server_addr, addr);

        let connection = self.endpoint.connect(addr, &self.server_name)?.await?;

        info!(
            "Connected to server: {} (0-RTT: {})",
            connection.remote_address(),
            connection
                .handshake_data()
                .map(|_| "enabled")
                .unwrap_or("disabled")
        );

        Ok(connection)
    }
}

fn normalize_server_name(server_name: &str) -> Option<String> {
    let trimmed = server_name.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(addr) = trimmed.parse::<SocketAddr>() {
        return Some(addr.ip().to_string());
    }

    let normalized = strip_host_port(trimmed).trim();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn strip_host_port(value: &str) -> &str {
    if let Some(rest) = value.strip_prefix('[') {
        if let Some((host, suffix)) = rest.split_once(']') {
            if suffix.is_empty() || suffix.starts_with(':') {
                return host;
            }
        }
    }

    if let Some((host, port)) = value.rsplit_once(':') {
        if !host.is_empty()
            && !port.is_empty()
            && port.chars().all(|ch| ch.is_ascii_digit())
            && !host.contains(':')
        {
            return host;
        }
    }

    value
}

#[cfg(test)]
mod tests {
    use super::normalize_server_name;

    #[test]
    fn normalize_server_name_strips_ipv6_brackets() {
        assert_eq!(normalize_server_name("[::1]"), Some("::1".to_string()));
        assert_eq!(
            normalize_server_name("[2001:db8::1]"),
            Some("2001:db8::1".to_string())
        );
    }

    #[test]
    fn normalize_server_name_rewrites_socket_addr_to_host() {
        assert_eq!(normalize_server_name("[::1]:9443"), Some("::1".to_string()));
        assert_eq!(
            normalize_server_name("127.0.0.1:9443"),
            Some("127.0.0.1".to_string())
        );
        assert_eq!(
            normalize_server_name("files.example.com:9443"),
            Some("files.example.com".to_string())
        );
        assert_eq!(
            normalize_server_name("[files.example.com]:9443"),
            Some("files.example.com".to_string())
        );
    }

    #[test]
    fn normalize_server_name_keeps_plain_host_and_rejects_blank() {
        assert_eq!(
            normalize_server_name(" files.example.com "),
            Some("files.example.com".to_string())
        );
        assert_eq!(normalize_server_name("   "), None);
    }
}
