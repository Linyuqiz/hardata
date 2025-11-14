use crate::sync::net::quic::QuicClient;
use crate::sync::net::tcp::TcpClient;
use anyhow::Result;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

pub struct HealthChecker {
    quic_client: Option<Arc<QuicClient>>,
    quic_connection: Option<Arc<quinn::Connection>>,
    tcp_client: Option<Arc<TcpClient>>,
    status_tx: watch::Sender<HealthStatus>,
    status_rx: watch::Receiver<HealthStatus>,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    consecutive_failures: AtomicU32,
    last_rtt_ms: AtomicU64,
    check_interval: Duration,
    failure_threshold: u32,
}

impl HealthChecker {
    pub fn new(
        quic_client: Option<Arc<QuicClient>>,
        quic_connection: Option<Arc<quinn::Connection>>,
        tcp_client: Option<Arc<TcpClient>>,
    ) -> Self {
        let (status_tx, status_rx) = watch::channel(HealthStatus::Healthy);

        Self {
            quic_client,
            quic_connection,
            tcp_client,
            status_tx,
            status_rx,
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            last_rtt_ms: AtomicU64::new(0),
            check_interval: Duration::from_secs(10),
            failure_threshold: 3,
        }
    }

    pub fn start(
        self: Arc<Self>,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                "Health checker started (interval: {}s, failure_threshold: {})",
                self.check_interval.as_secs(),
                self.failure_threshold
            );

            if let Err(e) = self.perform_check().await {
                warn!("Initial health check failed: {}", e);
            }

            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(self.check_interval).await;

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(e) = self.perform_check().await {
                    warn!("Health check failed: {}", e);
                }
            }

            info!("Health checker stopped");
        })
    }

    async fn perform_check(&self) -> Result<()> {
        let start = Instant::now();

        if let (Some(client), Some(conn)) = (&self.quic_client, &self.quic_connection) {
            match client.ping(conn).await {
                Ok(rtt_ms) => {
                    self.record_success(Some(rtt_ms));
                    info!("QUIC health check succeeded, RTT: {}ms", rtt_ms);
                    return Ok(());
                }
                Err(e) => {
                    self.record_failure();
                    warn!("QUIC health check failed: {}", e);
                    return Err(e.into());
                }
            }
        }

        if let Some(client) = &self.tcp_client {
            if let Some(status) = client.pool_status() {
                if status.size > 0 || status.available > 0 {
                    let elapsed = start.elapsed().as_millis() as u64;
                    self.record_success(Some(elapsed));
                    info!(
                        "TCP health check succeeded (pool: size={}, available={})",
                        status.size, status.available
                    );
                    return Ok(());
                } else {
                    self.record_failure();
                    warn!("TCP health check failed: no connections available");
                    return Err(anyhow::anyhow!("TCP pool has no connections"));
                }
            } else {
                self.record_failure();
                warn!("TCP health check failed: pool not configured");
                return Err(anyhow::anyhow!("TCP client has no pool"));
            }
        }

        self.record_failure();
        Err(anyhow::anyhow!("No connection available for health check"))
    }

    fn record_success(&self, rtt_ms: Option<u64>) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        if let Some(rtt) = rtt_ms {
            self.last_rtt_ms.store(rtt, Ordering::Relaxed);
        }

        let _ = self.status_tx.send(HealthStatus::Healthy);
    }

    fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        let new_status = if failures >= self.failure_threshold {
            warn!(
                "Connection marked as Unhealthy (consecutive_failures: {})",
                failures
            );
            HealthStatus::Unhealthy
        } else {
            warn!(
                "Connection marked as Degraded (consecutive_failures: {})",
                failures
            );
            HealthStatus::Degraded
        };

        let _ = self.status_tx.send(new_status);
    }

    pub async fn get_status(&self) -> HealthStatus {
        *self.status_rx.borrow()
    }
}
