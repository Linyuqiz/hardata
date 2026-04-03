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
    quic_client: Option<QuicClient>,
    tcp_client: Option<TcpClient>,
    tcp_delay_ms: u64,
}

impl HappyEyeballs {
    pub fn new(
        quic_client: Option<QuicClient>,
        tcp_client: Option<TcpClient>,
        tcp_delay_ms: Option<u64>,
    ) -> Self {
        Self {
            quic_client,
            tcp_client,
            tcp_delay_ms: tcp_delay_ms.unwrap_or(250),
        }
    }

    pub async fn connect(&self) -> Result<ConnectionResult> {
        self.connect_with_retry(&RetryConfig::default()).await
    }

    pub async fn connect_with_retry(&self, retry_config: &RetryConfig) -> Result<ConnectionResult> {
        self.connect_with_retry_and_timeout(retry_config, None)
            .await
    }

    pub async fn connect_with_retry_and_timeout(
        &self,
        retry_config: &RetryConfig,
        attempt_timeout: Option<Duration>,
    ) -> Result<ConnectionResult> {
        debug!("Happy Eyeballs connection attempt (with retry)");
        debug!(
            "  QUIC: {}",
            self.quic_client
                .as_ref()
                .map(|client| client.server_addr())
                .unwrap_or("disabled")
        );
        debug!(
            "  TCP: {} (delay: {}ms)",
            self.tcp_client
                .as_ref()
                .map(|client| client.server_addr())
                .unwrap_or("disabled"),
            self.tcp_delay_ms
        );
        debug!("  Retry: max_retries={}", retry_config.max_retries);
        if let Some(timeout) = attempt_timeout {
            debug!("  Timeout per attempt: {}ms", timeout.as_millis());
        }

        retry_with_backoff("Happy Eyeballs connection", retry_config, || async {
            if let Some(timeout) = attempt_timeout {
                match tokio::time::timeout(timeout, self.connect_internal()).await {
                    Ok(result) => result,
                    Err(_) => Err(HarDataError::NetworkError(format!(
                        "Happy Eyeballs connection timed out after {}ms",
                        timeout.as_millis()
                    ))),
                }
            } else {
                self.connect_internal().await
            }
        })
        .await
    }

    async fn connect_internal(&self) -> Result<ConnectionResult> {
        match (&self.quic_client, &self.tcp_client) {
            (Some(quic_client), Some(tcp_client)) => {
                let quic_client = quic_client.clone();
                let tcp_client = tcp_client.clone();

                let quic_future =
                    async move { quic_client.connect().await.map(ConnectionResult::Quic) };
                let tcp_future = async move {
                    tokio::time::sleep(Duration::from_millis(self.tcp_delay_ms)).await;
                    tcp_client.connect().await.map(ConnectionResult::Tcp)
                };

                tokio::pin!(quic_future);
                tokio::pin!(tcp_future);

                let mut quic_error = None;
                let mut tcp_error = None;
                let mut quic_done = false;
                let mut tcp_done = false;

                loop {
                    tokio::select! {
                        result = &mut quic_future, if !quic_done => {
                            quic_done = true;
                            match result {
                                Ok(conn) => {
                                    debug!("Using QUIC");
                                    return Ok(conn);
                                }
                                Err(e) => {
                                    warn!("QUIC connection failed: {}", e);
                                    quic_error = Some(e);
                                }
                            }
                        }
                        result = &mut tcp_future, if !tcp_done => {
                            tcp_done = true;
                            match result {
                                Ok(conn) => {
                                    debug!("Using TCP fallback");
                                    return Ok(conn);
                                }
                                Err(e) => {
                                    warn!("TCP connection failed: {}", e);
                                    tcp_error = Some(e);
                                }
                            }
                        }
                    }

                    if quic_done && tcp_done {
                        break;
                    }
                }

                Err(HarDataError::NetworkError(format!(
                    "Both QUIC and TCP connections failed (quic: {}; tcp: {})",
                    quic_error
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "not attempted".to_string()),
                    tcp_error
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "not attempted".to_string())
                )))
            }
            (Some(quic_client), None) => {
                debug!("TCP fallback disabled, trying QUIC only");
                Ok(ConnectionResult::Quic(quic_client.connect().await?))
            }
            (None, Some(tcp_client)) => {
                debug!("QUIC unavailable, using TCP only");
                Ok(ConnectionResult::Tcp(tcp_client.connect().await?))
            }
            (None, None) => Err(HarDataError::NetworkError(
                "No QUIC or TCP client configured".to_string(),
            )),
        }
    }

    pub async fn try_quic_only(&self, timeout_secs: u64) -> Result<quinn::Connection> {
        self.try_quic_only_with_timeout(Duration::from_secs(timeout_secs))
            .await
    }

    pub async fn try_quic_only_with_timeout(&self, timeout: Duration) -> Result<quinn::Connection> {
        info!(
            "Attempting QUIC connection only (timeout: {}ms)",
            timeout.as_millis()
        );

        let quic_client = self.quic_client.as_ref().ok_or_else(|| {
            HarDataError::InvalidConfig("QUIC is not configured for this region".to_string())
        })?;

        match tokio::time::timeout(timeout, quic_client.connect()).await {
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
        self.try_tcp_only_with_timeout(Duration::from_secs(timeout_secs))
            .await
    }

    pub async fn try_tcp_only_with_timeout(&self, timeout: Duration) -> Result<TcpStream> {
        info!(
            "Attempting TCP connection only (timeout: {}ms)",
            timeout.as_millis()
        );

        let tcp_client = self.tcp_client.as_ref().ok_or_else(|| {
            HarDataError::InvalidConfig("TCP is not configured for this region".to_string())
        })?;

        match tokio::time::timeout(timeout, tcp_client.connect()).await {
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
