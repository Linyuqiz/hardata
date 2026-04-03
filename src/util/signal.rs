use tokio::signal;
use tracing::info;

/// 等待 SIGINT 或 SIGTERM，返回收到的信号名称
pub async fn shutdown_signal() -> &'static str {
    let sigterm = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received SIGINT");
            "SIGINT"
        }
        _ = sigterm => {
            info!("Received SIGTERM");
            "SIGTERM"
        }
    }
}
