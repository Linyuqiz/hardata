use crate::core::constants::EWMA_BANDWIDTH_ALPHA;
use crate::sync::net::bandwidth::{BandwidthProbe, NetworkQuality};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info};

pub struct AdaptiveConfig {
    pub initial_concurrency: usize,
    pub min_concurrency: usize,
    pub max_concurrency: usize,
    pub increase_step: usize,
    pub decrease_factor: f64,
    pub sample_window: Duration,
    pub target_error_rate: f64,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_concurrency: 16,
            min_concurrency: 4,
            max_concurrency: 64,
            increase_step: 2,
            decrease_factor: 0.5,
            sample_window: Duration::from_secs(5),
            target_error_rate: 0.01,
        }
    }
}

pub struct AdaptiveController {
    config: AdaptiveConfig,
    current_concurrency: AtomicUsize,
    total_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_bytes: AtomicU64,
    last_sample_time: Mutex<Instant>,
    last_throughput: AtomicU64,
}

impl AdaptiveController {
    pub fn new(config: AdaptiveConfig) -> Arc<Self> {
        let initial = config.initial_concurrency;
        info!(
            "AdaptiveController created: initial={}, min={}, max={}",
            initial, config.min_concurrency, config.max_concurrency
        );

        Arc::new(Self {
            config,
            current_concurrency: AtomicUsize::new(initial),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            last_sample_time: Mutex::new(Instant::now()),
            last_throughput: AtomicU64::new(0),
        })
    }

    pub fn get_concurrency(&self) -> usize {
        self.current_concurrency.load(Ordering::Relaxed)
    }

    pub fn record_success(&self, bytes: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn adjust(&self) {
        let mut last_time = self.last_sample_time.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_time);

        if elapsed < self.config.sample_window {
            return;
        }

        let total = self.total_requests.swap(0, Ordering::Relaxed);
        let failed = self.failed_requests.swap(0, Ordering::Relaxed);
        let bytes = self.total_bytes.swap(0, Ordering::Relaxed);

        if total == 0 {
            *last_time = now;
            return;
        }

        let error_rate = failed as f64 / total as f64;
        let throughput = bytes * 1000 / elapsed.as_millis() as u64;
        let last_throughput = self.last_throughput.swap(throughput, Ordering::Relaxed);

        let current = self.current_concurrency.load(Ordering::Relaxed);

        let new_concurrency = if error_rate > self.config.target_error_rate {
            let new = (current as f64 * self.config.decrease_factor) as usize;
            let new = new.max(self.config.min_concurrency);
            info!(
                "AdaptiveController: error_rate={:.2}%, reducing concurrency {} -> {}",
                error_rate * 100.0,
                current,
                new
            );
            new
        } else if throughput > last_throughput || last_throughput == 0 {
            let new = (current + self.config.increase_step).min(self.config.max_concurrency);
            debug!(
                "AdaptiveController: throughput improved {} -> {} KB/s, increasing concurrency {} -> {}",
                last_throughput / 1024,
                throughput / 1024,
                current,
                new
            );
            new
        } else {
            debug!(
                "AdaptiveController: throughput stable at {} KB/s, keeping concurrency {}",
                throughput / 1024,
                current
            );
            current
        };

        self.current_concurrency
            .store(new_concurrency, Ordering::Relaxed);
        *last_time = now;
    }
}

pub struct NetworkAdaptiveController {
    bandwidth_probe: Arc<BandwidthProbe>,
    total_bytes_transferred: AtomicU64,
    total_chunks_transferred: AtomicU64,
    total_failures: AtomicU64,
}

impl NetworkAdaptiveController {
    pub fn new(bandwidth_probe: Arc<BandwidthProbe>) -> Arc<Self> {
        info!("NetworkAdaptiveController created");

        Arc::new(Self {
            bandwidth_probe,
            total_bytes_transferred: AtomicU64::new(0),
            total_chunks_transferred: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
        })
    }

    pub async fn assess_network_quality(&self) -> NetworkQuality {
        let bandwidth = self
            .bandwidth_probe
            .ewma_bandwidth_mbps(EWMA_BANDWIDTH_ALPHA);
        NetworkQuality::from_bandwidth_mbps(bandwidth)
    }

    pub fn record_transfer_success(&self, bytes: u64, duration: Duration) {
        self.total_bytes_transferred
            .fetch_add(bytes, Ordering::Relaxed);
        self.total_chunks_transferred
            .fetch_add(1, Ordering::Relaxed);
        self.bandwidth_probe.record_transfer(bytes, duration);
    }

    pub fn record_transfer_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
    }
}
