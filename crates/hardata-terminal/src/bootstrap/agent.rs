use hardata_infra_agent::agent_server as server;
use hardata_infra_agent::compute;
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

type ServerTask = JoinHandle<hardata_app::shared::error::Result<()>>;

fn abort_server_task(task: &mut Option<ServerTask>) {
    if let Some(handle) = task.take() {
        handle.abort();
    }
}

fn log_server_task_exit(
    server_name: &str,
    result: std::result::Result<hardata_app::shared::error::Result<()>, tokio::task::JoinError>,
) {
    match result {
        Ok(Ok(())) => error!(
            operation = "agent.server_stopped_unexpectedly",
            server = %server_name,
            "agent server stopped unexpectedly"
        ),
        Ok(Err(err)) => error!(
            operation = "agent.server_failed",
            server = %server_name,
            error = %err,
            "agent server failed"
        ),
        Err(err) => error!(
            operation = "agent.server_task_failed",
            server = %server_name,
            error = %err,
            "agent server task failed"
        ),
    }
}

async fn initialize_agent_servers(
    quic_bind: &str,
    tcp_bind: &str,
    compute_service: Arc<compute::ComputeService>,
    data_dir: &str,
    quic_cert_hostnames: &[String],
) -> hardata_app::shared::error::Result<(
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
                operation = "agent.quic_init_failed",
                error = %err,
                fallback = "tcp",
                "QUIC server initialization failed"
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
                operation = "agent.tcp_init_failed",
                error = %err,
                fallback = "quic",
                "TCP server initialization failed"
            );
            tcp_error = Some(err.to_string());
            None
        }
    };

    if quic_server.is_none() && tcp_server.is_none() {
        return Err(hardata_app::shared::error::HarDataError::NetworkError(
            format!(
                "All agent servers failed to initialize (QUIC: {}, TCP: {})",
                quic_error.unwrap_or_else(|| "unavailable".to_string()),
                tcp_error.unwrap_or_else(|| "unavailable".to_string())
            ),
        ));
    }

    Ok((quic_server, tcp_server))
}

async fn wait_for_agent_servers(
    mut quic_task: Option<ServerTask>,
    mut tcp_task: Option<ServerTask>,
) -> hardata_app::shared::error::Result<()> {
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
                    signal = hardata_app::shared::signal::shutdown_signal() => {
                        info!(
                            operation = "agent.shutdown_requested",
                            signal = %signal,
                            "agent shutdown requested"
                        );
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
                    signal = hardata_app::shared::signal::shutdown_signal() => {
                        info!(
                            operation = "agent.shutdown_requested",
                            signal = %signal,
                            "agent shutdown requested"
                        );
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
                    signal = hardata_app::shared::signal::shutdown_signal() => {
                        info!(
                            operation = "agent.shutdown_requested",
                            signal = %signal,
                            "agent shutdown requested"
                        );
                        abort_server_task(&mut tcp_task);
                        return Ok(());
                    }
                }
            }
            (None, None) => {
                return Err(hardata_app::shared::error::HarDataError::NetworkError(
                    "All agent servers stopped".to_string(),
                ));
            }
        }
    }
}

pub async fn run_agent(config_path: String) -> hardata_app::shared::error::Result<()> {
    let config_content = tokio::fs::read_to_string(&config_path).await.map_err(|e| {
        error!(
            operation = "agent.config_read_failed",
            config_path = %config_path,
            error = %e,
            "agent configuration read failed"
        );
        hardata_app::shared::error::HarDataError::Io(e)
    })?;

    let hardata_config: HarDataConfig = serde_yaml::from_str(&config_content).map_err(|e| {
        error!(
            operation = "agent.config_parse_failed",
            config_path = %config_path,
            error = %e,
            "agent configuration parse failed"
        );
        hardata_app::shared::error::HarDataError::InvalidConfig(format!("Invalid YAML: {}", e))
    })?;

    let config = hardata_config.agent;
    let quic_bind = config.quic_bind;
    let tcp_bind = config.tcp_bind;
    let data_dir = config.data_dir;
    let quic_cert_hostnames = config.quic_cert_hostnames;

    if !std::path::Path::new(&data_dir).exists() {
        std::fs::create_dir_all(&data_dir).map_err(hardata_app::shared::error::HarDataError::Io)?;
    }

    info!(
        operation = "agent.starting",
        quic_bind = %quic_bind,
        tcp_bind = %tcp_bind,
        data_dir = %data_dir,
        "agent starting"
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
        operation = "agent.ready",
        quic_enabled = quic_handle.is_some(),
        tcp_enabled = tcp_handle.is_some(),
        "agent ready"
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

    struct RuntimeDirectoryGuard {
        path: PathBuf,
        existed_before_test: bool,
    }

    impl RuntimeDirectoryGuard {
        fn new(path: PathBuf) -> Self {
            Self {
                existed_before_test: path.exists(),
                path,
            }
        }
    }

    impl Drop for RuntimeDirectoryGuard {
        fn drop(&mut self) {
            if !self.existed_before_test {
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
    }

    #[tokio::test]
    async fn wait_for_agent_servers_keeps_running_while_one_server_remains() {
        let (tx, rx) = oneshot::channel::<()>();

        let quic_task =
            tokio::spawn(async { Ok::<_, hardata_app::shared::error::HarDataError>(()) });
        let tcp_task = tokio::spawn(async move {
            let _ = rx.await;
            Ok::<_, hardata_app::shared::error::HarDataError>(())
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
        let _runtime_directory =
            RuntimeDirectoryGuard::new(std::env::current_dir().unwrap().join(".hardata"));
        let root = create_temp_dir("quic-only");
        let occupied_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_bind = occupied_tcp.local_addr().unwrap().to_string();

        let reserved_quic = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let quic_bind = reserved_quic.local_addr().unwrap().to_string();
        drop(reserved_quic);

        let compute = Arc::new(
            hardata_infra_agent::compute::ComputeService::new(root.to_str().unwrap())
                .await
                .unwrap(),
        );

        let (quic_server, tcp_server) = initialize_agent_servers(
            &quic_bind,
            &tcp_bind,
            compute,
            root.to_str().unwrap(),
            &["quic-only.test".to_string()],
        )
        .await
        .unwrap();

        assert!(quic_server.is_some());
        assert!(tcp_server.is_none());

        drop(occupied_tcp);
        let _ = std::fs::remove_dir_all(root);
    }
}
