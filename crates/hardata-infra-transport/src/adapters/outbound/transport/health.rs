use crate::adapters::outbound::transport::quic::QuicClient;
use crate::adapters::outbound::transport::tcp::TcpClient;
use anyhow::Result;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

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
                operation = "transport.health_started",
                interval_secs = self.check_interval.as_secs(),
                failure_threshold = self.failure_threshold,
                "transport health checker started"
            );

            if let Err(e) = self.perform_check().await {
                warn!(operation = "transport.health_check_failed", phase = "initial", error = %e, "transport health check failed");
            }

            while !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(self.check_interval).await;

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(e) = self.perform_check().await {
                    warn!(operation = "transport.health_check_failed", phase = "periodic", error = %e, "transport health check failed");
                }
            }
        })
    }

    async fn perform_check(&self) -> Result<()> {
        if let (Some(client), Some(conn)) = (&self.quic_client, &self.quic_connection) {
            match client.ping(conn).await {
                Ok(rtt_ms) => {
                    self.record_success(Some(rtt_ms));
                    debug!(
                        operation = "transport.health_check_succeeded",
                        protocol = "quic",
                        rtt_ms,
                        "transport health check succeeded"
                    );
                    return Ok(());
                }
                Err(e) => {
                    self.record_failure();
                    warn!(operation = "transport.health_check_failed", protocol = "quic", error = %e, "transport health check failed");
                    return Err(e.into());
                }
            }
        }

        if let Some(client) = &self.tcp_client {
            match client.ping().await {
                Ok(rtt_ms) => {
                    self.record_success(Some(rtt_ms));
                    debug!(
                        operation = "transport.health_check_succeeded",
                        protocol = "tcp",
                        rtt_ms,
                        "transport health check succeeded"
                    );
                    return Ok(());
                }
                Err(e) => {
                    self.record_failure();
                    warn!(operation = "transport.health_check_failed", protocol = "tcp", error = %e, "transport health check failed");
                    return Err(e.into());
                }
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
                operation = "transport.health_state_changed",
                status = "unhealthy",
                consecutive_failures = failures,
                "transport marked unhealthy"
            );
            HealthStatus::Unhealthy
        } else {
            warn!(
                operation = "transport.health_state_changed",
                status = "degraded",
                consecutive_failures = failures,
                "transport marked degraded"
            );
            HealthStatus::Degraded
        };

        let _ = self.status_tx.send(new_status);
    }

    pub async fn get_status(&self) -> HealthStatus {
        *self.status_rx.borrow()
    }
}
