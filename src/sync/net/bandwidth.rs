use std::collections::VecDeque;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::core::constants::{MIN_SAMPLE_INTERVAL, SAMPLE_WINDOW_SIZE};

#[derive(Debug, Clone, Copy)]
pub struct BandwidthSample {
    pub bytes: u64,
    pub duration: Duration,
    pub timestamp: Instant,
}

impl BandwidthSample {
    pub fn bandwidth_bps(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            (self.bytes as f64 * 8.0) / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn bandwidth_mbps(&self) -> f64 {
        self.bandwidth_bps() / 1_000_000.0
    }
}

pub struct BandwidthProbe {
    samples: RwLock<VecDeque<BandwidthSample>>,
    last_sample_time: RwLock<Instant>,
}

impl Default for BandwidthProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl BandwidthProbe {
    pub fn new() -> Self {
        Self {
            samples: RwLock::new(VecDeque::with_capacity(SAMPLE_WINDOW_SIZE)),
            last_sample_time: RwLock::new(Instant::now()),
        }
    }

    pub fn record_transfer(&self, bytes: u64, duration: Duration) {
        let now = Instant::now();
        let should_add_sample = {
            let last_time = self.last_sample_time.read().unwrap();
            now.duration_since(*last_time) >= MIN_SAMPLE_INTERVAL
        };

        if should_add_sample && duration.as_millis() > 0 {
            let sample = BandwidthSample {
                bytes,
                duration,
                timestamp: now,
            };

            let mut samples = self.samples.write().unwrap();
            if samples.len() >= SAMPLE_WINDOW_SIZE {
                samples.pop_front();
            }
            samples.push_back(sample);

            *self.last_sample_time.write().unwrap() = now;
        }
    }

    pub fn ewma_bandwidth_mbps(&self, alpha: f64) -> f64 {
        let samples = self.samples.read().unwrap();
        if samples.is_empty() {
            return 0.0;
        }

        let alpha = alpha.clamp(0.0, 1.0);
        let mut ewma = samples[0].bandwidth_mbps();

        for sample in samples.iter().skip(1) {
            ewma = alpha * sample.bandwidth_mbps() + (1.0 - alpha) * ewma;
        }

        ewma
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NetworkQuality {
    Excellent,
    Good,
    Fair,
    Poor,
    Unknown,
}

impl NetworkQuality {
    pub fn from_bandwidth_mbps(bw: f64) -> Self {
        match bw {
            bw if bw >= 800.0 => NetworkQuality::Excellent,
            bw if bw >= 200.0 => NetworkQuality::Good,
            bw if bw >= 50.0 => NetworkQuality::Fair,
            bw if bw > 0.0 => NetworkQuality::Poor,
            _ => NetworkQuality::Unknown,
        }
    }
}
