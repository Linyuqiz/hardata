use tokio::signal;
use tracing::info;

/// Wait for SIGINT or SIGTERM and return its name.
pub async fn shutdown_signal() -> &'static str {
    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!(operation = "process.shutdown_signal", signal = "SIGINT", "shutdown signal received");
            "SIGINT"
        }
        _ = sigterm => {
            info!(operation = "process.shutdown_signal", signal = "SIGTERM", "shutdown signal received");
            "SIGTERM"
        }
    }
}
