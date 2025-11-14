pub mod compute;
pub mod server;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
}

fn default_agent_data_dir() -> String {
    "/".to_string()
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

    info!(
        "Agent starting: quic={}, tcp={}, data_dir={}",
        quic_bind, tcp_bind, data_dir
    );

    let compute_service = Arc::new(compute::ComputeService::new(&data_dir).await?);

    let quic_server =
        match server::quic::QuicServer::new(&quic_bind, compute_service.clone(), &data_dir).await {
            Ok(server) => Some(server),
            Err(e) => {
                error!("QUIC server init failed: {}, using TCP-only", e);
                None
            }
        };

    let tcp_server =
        server::tcp::TcpServer::new(&tcp_bind, compute_service.clone(), &data_dir).await?;

    let quic_handle = quic_server.map(|server| {
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!("QUIC server error: {}", e);
            }
        })
    });

    let tcp_handle = {
        let server = tcp_server;
        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!("TCP server error: {}", e);
            }
        })
    };

    info!("Agent ready: quic={}, tcp={}", quic_handle.is_some(), true);

    if let Some(quic_h) = quic_handle {
        tokio::select! {
            _ = quic_h => {}
            _ = tcp_handle => {}
            _ = tokio::signal::ctrl_c() => {
                info!("Agent shutting down");
            }
        }
    } else {
        tokio::select! {
            _ = tcp_handle => {}
            _ = tokio::signal::ctrl_c() => {
                info!("Agent shutting down");
            }
        }
    }

    Ok(())
}
