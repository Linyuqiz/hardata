use crate::sync::net::bandwidth::NetworkQuality;
use crate::util::error::{HarDataError, Result};
use quinn::Endpoint;
use std::collections::HashMap;
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
    pub(super) ticket_store: Arc<SessionTicketStore>,
}

impl QuicClient {
    pub fn new(server_addr: String) -> Result<Self> {
        Self::new_with_quality(server_addr, NetworkQuality::Good)
    }

    pub fn new_with_quality(server_addr: String, quality: NetworkQuality) -> Result<Self> {
        info!(
            "Creating QUIC client for server: {} (quality: {:?})",
            server_addr, quality
        );

        let addr_clean = server_addr
            .strip_prefix("quic://")
            .unwrap_or(&server_addr)
            .to_string();

        let _ = rustls::crypto::ring::default_provider().install_default();

        let client_config = configure_client_for_quality(quality)?;

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_config);

        Ok(Self {
            endpoint,
            server_addr: addr_clean,
            ticket_store: SessionTicketStore::new(),
        })
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

        let host = self.server_addr.split(':').next().ok_or_else(|| {
            HarDataError::InvalidConfig(format!(
                "Invalid server address format: {}",
                self.server_addr
            ))
        })?;

        let connection = self.endpoint.connect(addr, host)?.await?;

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
