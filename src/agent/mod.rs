pub mod compute;
pub mod server;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, Deserialize, Serialize)]
pub struct HarDataConfig {
    pub agent: AgentConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AgentConfig {
    pub quic_bind: String,
    pub tcp_bind: String,
    #[serde(default = "default_agent_data_dir")]
    pub data_dir: String,
    #[serde(default)]
    pub quic_cert_hostnames: Vec<String>,
}

fn default_agent_data_dir() -> String {
    ".hardata/agent".to_string()
}

type ServerTask = JoinHandle<crate::util::error::Result<()>>;

fn abort_server_task(task: &mut Option<ServerTask>) {
    if let Some(handle) = task.take() {
        handle.abort();
    }
}

fn log_server_task_exit(
    server_name: &str,
    result: std::result::Result<crate::util::error::Result<()>, tokio::task::JoinError>,
) {
    match result {
        Ok(Ok(())) => error!("{} server stopped unexpectedly", server_name),
        Ok(Err(err)) => error!("{} server error: {}", server_name, err),
        Err(err) => error!("{} server task failed: {}", server_name, err),
    }
}

async fn initialize_agent_servers(
    quic_bind: &str,
    tcp_bind: &str,
    compute_service: Arc<compute::ComputeService>,
    data_dir: &str,
    quic_cert_hostnames: &[String],
) -> crate::util::error::Result<(
    Option<server::quic::QuicServer>,
    Option<server::tcp::TcpServer>,
)> {
    let mut quic_error = None;
    let quic_server = match server::quic::QuicServer::new(
        quic_bind,
        compute_service.clone(),
        data_dir,
        quic_cert_hostnames,
    )
    .await
    {
        Ok(server) => Some(server),
        Err(err) => {
            error!(
                "QUIC server init failed: {}, using TCP-only if available",
                err
            );
            quic_error = Some(err.to_string());
            None
        }
    };

    let mut tcp_error = None;
    let tcp_server = match server::tcp::TcpServer::new(tcp_bind, compute_service, data_dir).await {
        Ok(server) => Some(server),
        Err(err) => {
            error!(
                "TCP server init failed: {}, using QUIC-only if available",
                err
            );
            tcp_error = Some(err.to_string());
            None
        }
    };

    if quic_server.is_none() && tcp_server.is_none() {
        return Err(crate::util::error::HarDataError::NetworkError(format!(
            "All agent servers failed to initialize (QUIC: {}, TCP: {})",
            quic_error.unwrap_or_else(|| "unavailable".to_string()),
            tcp_error.unwrap_or_else(|| "unavailable".to_string())
        )));
    }

    Ok((quic_server, tcp_server))
}

async fn wait_for_agent_servers(
    mut quic_task: Option<ServerTask>,
    mut tcp_task: Option<ServerTask>,
) -> crate::util::error::Result<()> {
    loop {
        match (quic_task.as_mut(), tcp_task.as_mut()) {
            (Some(quic), Some(tcp)) => {
                tokio::select! {
                    result = quic => {
                        log_server_task_exit("QUIC", result);
                        quic_task = None;
                    }
                    result = tcp => {
                        log_server_task_exit("TCP", result);
                        tcp_task = None;
                    }
                    signal = crate::util::signal::shutdown_signal() => {
                        info!("Agent received {}, shutting down", signal);
                        abort_server_task(&mut quic_task);
                        abort_server_task(&mut tcp_task);
                        return Ok(());
                    }
                }
            }
            (Some(quic), None) => {
                tokio::select! {
                    result = quic => {
                        log_server_task_exit("QUIC", result);
                        quic_task = None;
                    }
                    signal = crate::util::signal::shutdown_signal() => {
                        info!("Agent received {}, shutting down", signal);
                        abort_server_task(&mut quic_task);
                        return Ok(());
                    }
                }
            }
            (None, Some(tcp)) => {
                tokio::select! {
                    result = tcp => {
                        log_server_task_exit("TCP", result);
                        tcp_task = None;
                    }
                    signal = crate::util::signal::shutdown_signal() => {
                        info!("Agent received {}, shutting down", signal);
                        abort_server_task(&mut tcp_task);
                        return Ok(());
                    }
                }
            }
            (None, None) => {
                return Err(crate::util::error::HarDataError::NetworkError(
                    "All agent servers stopped".to_string(),
                ));
            }
        }
    }
}

pub async fn run_agent(config_path: String) -> crate::util::error::Result<()> {
    let config_content = tokio::fs::read_to_string(&config_path).await.map_err(|e| {
        error!("Failed to read config: {}", e);
        crate::util::error::HarDataError::Io(e)
    })?;

    let hardata_config: HarDataConfig = serde_yaml::from_str(&config_content).map_err(|e| {
        error!("Failed to parse config: {}", e);
        crate::util::error::HarDataError::InvalidConfig(format!("Invalid YAML: {}", e))
    })?;

    let config = hardata_config.agent;
    let quic_bind = config.quic_bind;
    let tcp_bind = config.tcp_bind;
    let data_dir = config.data_dir;
    let quic_cert_hostnames = config.quic_cert_hostnames;

    if !std::path::Path::new(&data_dir).exists() {
        std::fs::create_dir_all(&data_dir).map_err(crate::util::error::HarDataError::Io)?;
    }

    info!(
        "Agent starting: quic={}, tcp={}, data_dir={}",
        quic_bind, tcp_bind, data_dir
    );

    let compute_service = Arc::new(compute::ComputeService::new(&data_dir).await?);

    let (quic_server, tcp_server) = initialize_agent_servers(
        &quic_bind,
        &tcp_bind,
        compute_service.clone(),
        &data_dir,
        &quic_cert_hostnames,
    )
    .await?;

    let quic_handle = quic_server.map(|server| tokio::spawn(async move { server.run().await }));
    let tcp_handle = tcp_server.map(|server| tokio::spawn(async move { server.run().await }));

    info!(
        "Agent ready: quic={}, tcp={}",
        quic_handle.is_some(),
        tcp_handle.is_some()
    );

    wait_for_agent_servers(quic_handle, tcp_handle).await
}

#[cfg(test)]
mod tests {
    use super::{initialize_agent_servers, wait_for_agent_servers};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout, Duration};

    fn create_temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("hardata-agent-{label}-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn wait_for_agent_servers_keeps_running_while_one_server_remains() {
        let (tx, rx) = oneshot::channel::<()>();

        let quic_task = tokio::spawn(async { Ok::<_, crate::util::error::HarDataError>(()) });
        let tcp_task = tokio::spawn(async move {
            let _ = rx.await;
            Ok::<_, crate::util::error::HarDataError>(())
        });

        let wait_task = tokio::spawn(wait_for_agent_servers(Some(quic_task), Some(tcp_task)));
        sleep(Duration::from_millis(100)).await;
        assert!(!wait_task.is_finished());

        let _ = tx.send(());
        let result = timeout(Duration::from_secs(1), wait_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(result.to_string().contains("All agent servers stopped"));
    }

    #[tokio::test]
    async fn initialize_agent_servers_allows_quic_only_when_tcp_bind_fails() {
        let root = create_temp_dir("quic-only");
        let occupied_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_bind = occupied_tcp.local_addr().unwrap().to_string();

        let reserved_quic = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let quic_bind = reserved_quic.local_addr().unwrap().to_string();
        drop(reserved_quic);

        let compute = Arc::new(
            crate::agent::compute::ComputeService::new(root.to_str().unwrap())
                .await
                .unwrap(),
        );

        let (quic_server, tcp_server) =
            initialize_agent_servers(&quic_bind, &tcp_bind, compute, root.to_str().unwrap(), &[])
                .await
                .unwrap();

        assert!(quic_server.is_some());
        assert!(tcp_server.is_none());

        drop(occupied_tcp);
        let _ = std::fs::remove_dir_all(root);
    }
}
