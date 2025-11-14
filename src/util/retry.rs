use std::time::Duration;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 10000,
            backoff_multiplier: 2.0,
        }
    }
}

pub async fn retry_with_backoff<F, Fut, T, E>(
    operation_name: &str,
    config: &RetryConfig,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    let mut delay_ms = config.initial_delay_ms;

    loop {
        attempt += 1;

        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    debug!("{} succeeded after {} attempts", operation_name, attempt);
                }
                return Ok(result);
            }
            Err(e) => {
                if attempt >= config.max_retries {
                    warn!(
                        "{} failed after {} attempts: {}",
                        operation_name, attempt, e
                    );
                    return Err(e);
                }

                warn!(
                    "{} failed (attempt {}/{}): {}. Retrying in {}ms...",
                    operation_name, attempt, config.max_retries, e, delay_ms
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                delay_ms =
                    ((delay_ms as f64 * config.backoff_multiplier) as u64).min(config.max_delay_ms);
            }
        }
    }
}
