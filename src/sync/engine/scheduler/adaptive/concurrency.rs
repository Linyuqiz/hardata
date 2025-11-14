use crate::sync::net::bandwidth::NetworkQuality;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, info};

pub struct AdaptiveConcurrencyController {
    current_concurrency: AtomicUsize,
    min_concurrency: usize,
    max_concurrency: usize,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    additive_increase: usize,
    multiplicative_decrease: f64,
}

impl AdaptiveConcurrencyController {
    pub fn new(min: usize, max: usize) -> Arc<Self> {
        let initial = Self::initial_concurrency_for_quality(NetworkQuality::Unknown);
        info!(
            "AdaptiveConcurrencyController created: initial={}, min={}, max={}",
            initial, min, max
        );

        Arc::new(Self {
            current_concurrency: AtomicUsize::new(initial.clamp(min, max)),
            min_concurrency: min,
            max_concurrency: max,
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            additive_increase: 2,
            multiplicative_decrease: 0.5,
        })
    }

    pub fn initial_concurrency_for_quality(quality: NetworkQuality) -> usize {
        match quality {
            NetworkQuality::Excellent => 64,
            NetworkQuality::Good => 32,
            NetworkQuality::Fair => 16,
            NetworkQuality::Poor => 4,
            NetworkQuality::Unknown => 8,
        }
    }

    pub fn get_concurrency(&self) -> usize {
        self.current_concurrency.load(Ordering::Relaxed)
    }

    pub fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn adjust_concurrency(&self, quality: NetworkQuality) -> usize {
        let success = self.success_count.swap(0, Ordering::Relaxed);
        let failure = self.failure_count.swap(0, Ordering::Relaxed);
        let current = self.current_concurrency.load(Ordering::Relaxed);

        let total = success + failure;
        let failure_rate = if total > 0 {
            failure as f64 / total as f64
        } else {
            0.0
        };

        let new_concurrency = if failure_rate > 0.1 {
            let new = (current as f64 * self.multiplicative_decrease) as usize;
            let new = new.max(self.min_concurrency);
            info!(
                "Concurrency: failure_rate={:.1}%, reducing {} -> {}",
                failure_rate * 100.0,
                current,
                new
            );
            new
        } else if quality == NetworkQuality::Excellent && success > 20 {
            let new = (current + self.additive_increase * 2).min(self.max_concurrency);
            debug!(
                "Concurrency: excellent network, increasing {} -> {}",
                current, new
            );
            new
        } else if quality == NetworkQuality::Good && success > 10 {
            let new = (current + self.additive_increase).min(self.max_concurrency);
            debug!(
                "Concurrency: good network, increasing {} -> {}",
                current, new
            );
            new
        } else if (quality == NetworkQuality::Fair || quality == NetworkQuality::Unknown)
            && failure_rate < 0.05
            && success > 5
        {
            let new = (current + 1).min(self.max_concurrency);
            debug!(
                "Concurrency: {:?} network low errors, increasing {} -> {}",
                quality, current, new
            );
            new
        } else if quality == NetworkQuality::Poor || quality == NetworkQuality::Unknown {
            let target = Self::initial_concurrency_for_quality(quality);
            if current > target {
                let new = ((current + target) / 2).max(self.min_concurrency);
                debug!(
                    "Concurrency: poor/unknown network, reducing {} -> {}",
                    current, new
                );
                new
            } else {
                current
            }
        } else {
            current
        };

        self.current_concurrency
            .store(new_concurrency, Ordering::Relaxed);
        new_concurrency
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConcurrencyStrategy {
    Fixed(usize),
    #[default]
    Adaptive,
    QualityBased,
}
