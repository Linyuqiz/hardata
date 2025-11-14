use crate::sync::net::quic::QuicClient;
use crate::sync::net::tcp::TcpClient;
use crate::util::error::{HarDataError, Result};
use crate::util::retry::{retry_with_backoff, RetryConfig};
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportProtocol {
    Quic,
    Tcp,
}

pub enum ConnectionResult {
    Quic(quinn::Connection),
    Tcp(TcpStream),
}

pub struct HappyEyeballs {
    quic_addr: String,
    tcp_addr: String,
    tcp_delay_ms: u64,
}

impl HappyEyeballs {
    pub fn new(quic_addr: String, tcp_addr: String, tcp_delay_ms: Option<u64>) -> Self {
        Self {
            quic_addr,
            tcp_addr,
            tcp_delay_ms: tcp_delay_ms.unwrap_or(250),
        }
    }

    pub async fn connect(&self) -> Result<ConnectionResult> {
        self.connect_with_retry(&RetryConfig::default()).await
    }

    pub async fn connect_with_retry(&self, retry_config: &RetryConfig) -> Result<ConnectionResult> {
        debug!("Happy Eyeballs connection attempt (with retry)");
        debug!("  QUIC: {}", self.quic_addr);
        debug!("  TCP: {} (delay: {}ms)", self.tcp_addr, self.tcp_delay_ms);
        debug!("  Retry: max_retries={}", retry_config.max_retries);

        retry_with_backoff("Happy Eyeballs connection", retry_config, || async {
            self.connect_internal().await
        })
        .await
    }

    async fn connect_internal(&self) -> Result<ConnectionResult> {
        let quic_client = QuicClient::new(self.quic_addr.clone())?;

        let tcp_client = TcpClient::new(self.tcp_addr.clone())?;

        let quic_future = async {
            match quic_client.connect().await {
                Ok(conn) => {
                    debug!("QUIC connected");
                    Some(ConnectionResult::Quic(conn))
                }
                Err(e) => {
                    warn!("QUIC connection failed: {}", e);
                    None
                }
            }
        };

        let tcp_future = async {
            tokio::time::sleep(Duration::from_millis(self.tcp_delay_ms)).await;

            match tcp_client.connect().await {
                Ok(stream) => {
                    debug!("TCP connected");
                    Some(ConnectionResult::Tcp(stream))
                }
                Err(e) => {
                    warn!("TCP connection failed: {}", e);
                    None
                }
            }
        };

        tokio::select! {
            quic_result = quic_future => {
                if let Some(conn) = quic_result {
                    debug!("Using QUIC");
                    return Ok(conn);
                }
            }
            tcp_result = tcp_future => {
                if let Some(conn) = tcp_result {
                    debug!("Using TCP fallback");
                    return Ok(conn);
                }
            }
        }

        Err(HarDataError::NetworkError(
            "Both QUIC and TCP connections failed".to_string(),
        ))
    }

    pub async fn try_quic_only(&self, timeout_secs: u64) -> Result<quinn::Connection> {
        info!(
            "Attempting QUIC connection only (timeout: {}s)",
            timeout_secs
        );

        let quic_client = QuicClient::new(self.quic_addr.clone())?;

        match tokio::time::timeout(Duration::from_secs(timeout_secs), quic_client.connect()).await {
            Ok(Ok(conn)) => {
                debug!("QUIC connected");
                Ok(conn)
            }
            Ok(Err(e)) => {
                warn!("QUIC connection failed: {}", e);
                Err(e)
            }
            Err(_) => {
                warn!("QUIC connection timed out");
                Err(HarDataError::NetworkError(
                    "QUIC connection timeout".to_string(),
                ))
            }
        }
    }

    pub async fn try_tcp_only(&self, timeout_secs: u64) -> Result<TcpStream> {
        info!(
            "Attempting TCP connection only (timeout: {}s)",
            timeout_secs
        );

        let tcp_client = TcpClient::new(self.tcp_addr.clone())?;

        match tokio::time::timeout(Duration::from_secs(timeout_secs), tcp_client.connect()).await {
            Ok(Ok(stream)) => {
                debug!("TCP connected");
                Ok(stream)
            }
            Ok(Err(e)) => {
                warn!("TCP connection failed: {}", e);
                Err(e)
            }
            Err(_) => {
                warn!("TCP connection timed out");
                Err(HarDataError::NetworkError(
                    "TCP connection timeout".to_string(),
                ))
            }
        }
    }
}
