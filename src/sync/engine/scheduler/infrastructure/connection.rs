use crate::sync::net::eyeballs::{ConnectionResult, HappyEyeballs, TransportProtocol};
use crate::sync::net::health::{HealthChecker, HealthStatus};
use crate::sync::net::transport::{Protocol, ProtocolSelector, TransportConnection};
use crate::sync::RegionConfig;
use crate::util::error::{HarDataError, Result};
use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::config::{ConnectionPool, SchedulerConfig};

use crate::core::constants::{MAX_RECONNECT_ATTEMPTS, RECONNECT_DELAY_MS, SCHEDULER_POOL_SIZE};

fn get_forced_protocol() -> Option<&'static str> {
    std::env::var("HARDATA_PROTOCOL")
        .ok()
        .and_then(|p| match p.to_lowercase().as_str() {
            "quic" => Some("quic"),
            "tcp" => Some("tcp"),
            _ => None,
        })
}

#[derive(Clone, Copy)]
enum ConnectionEstablishMode {
    Startup,
    RuntimeReconnect,
}

fn retry_config_for_mode(mode: ConnectionEstablishMode) -> crate::util::retry::RetryConfig {
    match mode {
        ConnectionEstablishMode::Startup => crate::util::retry::RetryConfig {
            max_retries: 1,
            initial_delay_ms: 0,
            max_delay_ms: 0,
            backoff_multiplier: 1.0,
        },
        ConnectionEstablishMode::RuntimeReconnect => crate::util::retry::RetryConfig::default(),
    }
}

fn connect_timeout_for_mode(mode: ConnectionEstablishMode) -> Duration {
    match mode {
        ConnectionEstablishMode::Startup => Duration::from_secs(3),
        ConnectionEstablishMode::RuntimeReconnect => Duration::from_secs(15),
    }
}

pub async fn establish_all_connections(
    config: &SchedulerConfig,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    info!(
        "Establishing connections for {} regions...",
        config.regions.len()
    );

    let mut failures = Vec::new();
    let mut successes = 0usize;

    for region in &config.regions {
        if let Some(pool) = connection_pools.get(&region.name) {
            match establish_region_connection_with_mode(
                region,
                &pool,
                shutdown,
                ConnectionEstablishMode::Startup,
            )
            .await
            {
                Ok(()) => successes += 1,
                Err(err) => {
                    warn!(
                        "Initial connection to region '{}' failed, deferring to lazy reconnect: {}",
                        region.name, err
                    );
                    failures.push(format!("{}: {}", region.name, err));
                }
            }
        }
    }

    if failures.is_empty() {
        info!("All region connections established successfully");
        Ok(())
    } else {
        Err(HarDataError::NetworkError(format!(
            "Established {}/{} regions during startup; deferred regions: {}",
            successes,
            config.regions.len(),
            failures.join("; ")
        )))
    }
}

pub async fn establish_region_connection(
    region: &RegionConfig,
    connection_pool: &Arc<Mutex<ConnectionPool>>,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    establish_region_connection_with_mode(
        region,
        connection_pool,
        shutdown,
        ConnectionEstablishMode::RuntimeReconnect,
    )
    .await
}

async fn establish_region_connection_with_mode(
    region: &RegionConfig,
    connection_pool: &Arc<Mutex<ConnectionPool>>,
    shutdown: &Arc<AtomicBool>,
    mode: ConnectionEstablishMode,
) -> Result<()> {
    let forced_protocol = get_forced_protocol();

    debug!("Connecting to region '{}'...", region.name);
    debug!("  QUIC: {}", region.quic_bind);
    debug!("  TCP: {}", region.tcp_bind);
    if let Some(proto) = forced_protocol {
        debug!("  Forced: {}", proto.to_uppercase());
    }

    let (quic_client, tcp_client) = {
        let mut pool = connection_pool.lock().await;
        // 重连时尝试重建 QUIC 客户端（证书可能在初始化后才生成）
        if pool.quic_client.is_none() {
            let quic_host = region.quic_bind.split(':').next().unwrap_or("").to_string();
            if !quic_host.is_empty() && quic_host != "0.0.0.0" {
                let safe_name = quic_host.replace(':', "-");
                let ca_cert_path = format!(".hardata/tls/agent-cert-{}.der", safe_name);
                if std::path::Path::new(&ca_cert_path).exists() {
                    match crate::sync::net::quic::QuicClient::new(
                        region.quic_bind.clone(),
                        quic_host.clone(),
                        ca_cert_path,
                    ) {
                        Ok(client) => {
                            info!(
                                "Region '{}' QUIC client recovered (certificate now available)",
                                region.name
                            );
                            pool.quic_client = Some(client);
                        }
                        Err(e) => {
                            debug!(
                                "Region '{}' QUIC client rebuild skipped: {}",
                                region.name, e
                            );
                        }
                    }
                }
            }
        }
        (pool.quic_client.clone(), pool.tcp_client.clone())
    };

    let connector = HappyEyeballs::new(quic_client, tcp_client, Some(250));
    let retry_config = retry_config_for_mode(mode);
    let connect_timeout = connect_timeout_for_mode(mode);

    let connection_result = match forced_protocol {
        Some("quic") => {
            debug!("Forcing QUIC-only connection for region '{}'", region.name);
            ConnectionResult::Quic(
                connector
                    .try_quic_only_with_timeout(connect_timeout)
                    .await?,
            )
        }
        Some("tcp") => {
            debug!("Forcing TCP-only connection for region '{}'", region.name);
            ConnectionResult::Tcp(connector.try_tcp_only_with_timeout(connect_timeout).await?)
        }
        _ => {
            connector
                .connect_with_retry_and_timeout(&retry_config, Some(connect_timeout))
                .await?
        }
    };

    match connection_result {
        ConnectionResult::Quic(conn) => {
            info!("Region '{}' connected via QUIC protocol", region.name);
            let mut pool = connection_pool.lock().await;
            pool.quic_connection = Some(conn.clone());
            pool.protocol = Some(TransportProtocol::Quic);

            let checker = Arc::new(HealthChecker::new(
                pool.quic_client.clone().map(Arc::new),
                Some(Arc::new(conn)),
                None,
            ));
            let health_task = checker.clone().start(Arc::clone(shutdown));
            pool.health_checker = Some(checker);
            pool.health_task = Some(health_task);
        }
        ConnectionResult::Tcp(_stream) => {
            info!("Region '{}' connected via TCP protocol", region.name);
            let mut pool = connection_pool.lock().await;
            pool.protocol = Some(TransportProtocol::Tcp);

            if pool.tcp_client.is_none() {
                debug!(
                    "Creating TCP client for region '{}' with pool_size={}",
                    region.name, SCHEDULER_POOL_SIZE
                );
                match crate::sync::net::tcp::TcpClient::with_pool(
                    region.tcp_bind.clone(),
                    Some(SCHEDULER_POOL_SIZE),
                ) {
                    Ok(client) => {
                        pool.tcp_client = Some(client);
                        info!(
                            "TCP client created successfully for region '{}'",
                            region.name
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create TCP client with pool for region '{}': {}, using simple client",
                            region.name, e
                        );
                        if let Ok(simple_client) =
                            crate::sync::net::tcp::TcpClient::new(region.tcp_bind.clone())
                        {
                            pool.tcp_client = Some(simple_client);
                            debug!("Simple TCP client for region '{}'", region.name);
                        }
                    }
                }
            }

            let checker = Arc::new(HealthChecker::new(
                None,
                None,
                pool.tcp_client.clone().map(Arc::new),
            ));
            let health_task = checker.clone().start(Arc::clone(shutdown));
            pool.health_checker = Some(checker);
            pool.health_task = Some(health_task);
        }
    }

    info!(
        "Region '{}' connection established successfully",
        region.name
    );
    Ok(())
}

pub async fn get_transport_connection(
    connection_pool: &Mutex<ConnectionPool>,
) -> Result<TransportConnection> {
    let pool = connection_pool.lock().await;

    if let Some(ref checker) = pool.health_checker {
        let status = checker.get_status().await;
        if status == HealthStatus::Unhealthy {
            return Err(HarDataError::NetworkError(
                "Connection is unhealthy, reconnection needed".to_string(),
            ));
        }
    }

    match pool.protocol {
        Some(TransportProtocol::Quic) => {
            if let Some(ref conn) = pool.quic_connection {
                if conn.close_reason().is_some() {
                    return Err(HarDataError::NetworkError(
                        "QUIC connection closed, need reconnect".to_string(),
                    ));
                }

                let client = pool
                    .quic_client
                    .as_ref()
                    .ok_or_else(|| {
                        HarDataError::NetworkError("QUIC client not available".to_string())
                    })?
                    .clone();

                debug!("Using QUIC connection for transfer");
                Ok(TransportConnection::Quic {
                    client,
                    connection: conn.clone(),
                })
            } else {
                Err(HarDataError::NetworkError(
                    "QUIC connection not established".to_string(),
                ))
            }
        }
        Some(TransportProtocol::Tcp) => {
            let client = pool
                .tcp_client
                .as_ref()
                .ok_or_else(|| HarDataError::NetworkError("TCP client not available".to_string()))?
                .clone();

            debug!("TCP client ready for concurrent connections");
            Ok(TransportConnection::Tcp { client })
        }
        None => Err(HarDataError::ProtocolError(
            "Not connected to server".to_string(),
        )),
    }
}

pub async fn get_connection_with_retry_for_region(
    config: &Arc<SchedulerConfig>,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    region: &str,
    shutdown: &Arc<AtomicBool>,
) -> Result<TransportConnection> {
    get_connection_with_retry_for_region_with_selector(
        config,
        connection_pools,
        region,
        shutdown,
        None,
    )
    .await
}

pub async fn get_connection_with_retry_for_region_with_selector(
    config: &Arc<SchedulerConfig>,
    connection_pools: &Arc<DashMap<String, Arc<Mutex<ConnectionPool>>>>,
    region: &str,
    shutdown: &Arc<AtomicBool>,
    protocol_selector: Option<&Arc<ProtocolSelector>>,
) -> Result<TransportConnection> {
    let connection_pool = connection_pools
        .get(region)
        .ok_or_else(|| HarDataError::InvalidConfig(format!("Region '{}' not found", region)))?
        .clone();

    let region_config = config
        .regions
        .iter()
        .find(|r| r.name == region)
        .ok_or_else(|| {
            HarDataError::InvalidConfig(format!("Region config '{}' not found", region))
        })?
        .clone();

    let mut last_error = None;

    for attempt in 0..MAX_RECONNECT_ATTEMPTS {
        match get_transport_connection(&connection_pool).await {
            Ok(conn) => {
                if let Some(selector) = protocol_selector {
                    let protocol = match &conn {
                        TransportConnection::Quic { .. } => Protocol::Quic,
                        TransportConnection::Tcp { .. } => Protocol::Tcp,
                    };
                    selector.record_result(protocol, true);
                }
                return Ok(conn);
            }
            Err(e) => {
                warn!(
                    "Region '{}' connection attempt {} failed: {}, trying reconnect...",
                    region,
                    attempt + 1,
                    e
                );
                last_error = Some(e);

                if let Some(selector) = protocol_selector {
                    let pool = connection_pool.lock().await;
                    let protocol = match pool.protocol {
                        Some(TransportProtocol::Quic) => Protocol::Quic,
                        Some(TransportProtocol::Tcp) => Protocol::Tcp,
                        None => Protocol::Quic,
                    };
                    drop(pool);
                    selector.record_result(protocol, false);
                }

                if let Err(reconnect_err) =
                    try_reconnect_region(&region_config, &connection_pool, shutdown).await
                {
                    warn!(
                        "Region '{}' reconnect attempt {} failed: {}",
                        region,
                        attempt + 1,
                        reconnect_err
                    );
                }

                tokio::time::sleep(std::time::Duration::from_millis(
                    RECONNECT_DELAY_MS * (attempt as u64 + 1),
                ))
                .await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        HarDataError::NetworkError(format!(
            "Failed to establish connection to region '{}' after retries",
            region
        ))
    }))
}

async fn try_reconnect_region(
    region: &RegionConfig,
    connection_pool: &Arc<Mutex<ConnectionPool>>,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    debug!("Attempting to reconnect to region '{}'...", region.name);

    {
        let mut pool = connection_pool.lock().await;

        if let Some(task) = pool.health_task.take() {
            task.abort();
        }

        pool.quic_connection = None;
        pool.health_checker = None;
    }

    establish_region_connection(region, connection_pool, shutdown).await?;

    info!("Region '{}' reconnection successful", region.name);
    Ok(())
}
