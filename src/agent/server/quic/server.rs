use crate::agent::compute::ComputeService;
use crate::core::protocol::{ISyncMessage, MessageType};
use crate::util::error::{HarDataError, Result};
use quinn::Endpoint;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};

use super::config::{
    configure_server, load_or_generate_self_signed_cert, MAX_CONCURRENT_CONNECTIONS,
    MAX_CONCURRENT_STREAMS, MAX_PAYLOAD_SIZE, REQUEST_TIMEOUT_SECS,
};
use super::handlers;

pub struct QuicServer {
    endpoint: Endpoint,
    compute: Arc<ComputeService>,
    data_dir: PathBuf,
    connection_semaphore: Arc<Semaphore>,
    stream_semaphore: Arc<Semaphore>,
    active_connections: Arc<AtomicUsize>,
}

impl QuicServer {
    pub async fn new(
        bind_addr: &str,
        compute: Arc<ComputeService>,
        data_dir: &str,
        certificate_hostnames: &[String],
    ) -> Result<Self> {
        let data_dir_path = PathBuf::from(data_dir);
        let (certs, key) =
            load_or_generate_self_signed_cert(bind_addr, &data_dir_path, certificate_hostnames)?;

        let server_config = configure_server(certs, key)?;

        let addr = bind_addr
            .parse()
            .map_err(|e| HarDataError::InvalidConfig(format!("Invalid bind address: {}", e)))?;

        let endpoint = Endpoint::server(server_config, addr)?;

        Ok(Self {
            endpoint,
            compute,
            data_dir: data_dir_path,
            connection_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
            stream_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "QUIC server started: max_conn={}, max_streams={}",
            MAX_CONCURRENT_CONNECTIONS, MAX_CONCURRENT_STREAMS
        );

        loop {
            match self.endpoint.accept().await {
                Some(incoming) => {
                    let permit = match self.connection_semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            warn!("Connection limit reached");
                            continue;
                        }
                    };

                    let compute = self.compute.clone();
                    let data_dir = self.data_dir.clone();
                    let stream_semaphore = self.stream_semaphore.clone();
                    let active_connections = self.active_connections.clone();

                    active_connections.fetch_add(1, Ordering::Relaxed);

                    tokio::spawn(async move {
                        match incoming.await {
                            Ok(connection) => {
                                if let Err(e) = handle_connection(
                                    connection,
                                    compute,
                                    data_dir,
                                    stream_semaphore,
                                )
                                .await
                                {
                                    error!("Connection error: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Connection failed: {}", e);
                            }
                        }
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                    });
                }
                None => {
                    warn!("Server stopped accepting connections");
                    break;
                }
            }
        }
        Ok(())
    }
}

async fn handle_connection(
    connection: quinn::Connection,
    compute: Arc<ComputeService>,
    data_dir: PathBuf,
    stream_semaphore: Arc<Semaphore>,
) -> Result<()> {
    debug!("New QUIC connection: {}", connection.remote_address());

    loop {
        match connection.accept_bi().await {
            Ok((send, recv)) => {
                let permit = match stream_semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!("Stream limit reached");
                        continue;
                    }
                };

                let compute = compute.clone();
                let data_dir = data_dir.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_stream(send, recv, compute, data_dir).await {
                        error!("Stream error: {}", e);
                    }
                    drop(permit);
                });
            }
            Err(e) => {
                warn!("Connection closed: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    compute: Arc<ComputeService>,
    data_dir: PathBuf,
) -> Result<()> {
    loop {
        let request_timeout = Duration::from_secs(REQUEST_TIMEOUT_SECS);

        let mut header = [0u8; 5];
        let header_read_result = match timeout(request_timeout, recv.read_exact(&mut header)).await
        {
            Ok(result) => result,
            Err(_) => {
                warn!("Request timeout after {} seconds", REQUEST_TIMEOUT_SECS);
                let error_msg = format!("Request timeout after {} seconds", REQUEST_TIMEOUT_SECS);
                let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                let _ = send.write_all(&response.encode()).await;
                return Err(HarDataError::NetworkError("Request timeout".to_string()));
            }
        };

        match header_read_result {
            Ok(_) => {}
            Err(quinn::ReadExactError::FinishedEarly(_)) => {
                tracing::debug!("Stream closed by peer (FinishedEarly), exiting loop");
                break;
            }
            Err(e) => {
                tracing::error!("Failed to read header: {}", e);
                return Err(HarDataError::NetworkError(format!(
                    "read_exact failed: {}",
                    e
                )));
            }
        }

        let msg_type = MessageType::from_u8(header[0])?;
        let payload_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

        if payload_len > MAX_PAYLOAD_SIZE {
            error!(
                "Payload size {} exceeds limit {}, rejecting request",
                payload_len, MAX_PAYLOAD_SIZE
            );
            let error_msg = format!(
                "Payload size {} exceeds maximum allowed size {}",
                payload_len, MAX_PAYLOAD_SIZE
            );
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            send.write_all(&response.encode()).await?;
            continue;
        }

        use crate::util::buffer_pool::global_buffer_pool;
        let pooled_buffer = if payload_len > 0 {
            let pool = global_buffer_pool();
            let mut buffer = pool.acquire();
            buffer.resize(payload_len, 0);
            recv.read_exact(&mut buffer[..payload_len]).await?;
            Some(buffer)
        } else {
            None
        };

        let payload_buf: &[u8] = match &pooled_buffer {
            Some(buf) => &buf[..payload_len],
            None => &[],
        };

        let is_ping = msg_type == MessageType::Ping;

        match msg_type {
            MessageType::ListDirectoryRequest => {
                handlers::handle_list_directory(&mut send, payload_buf, &compute, &data_dir)
                    .await?;
            }
            MessageType::GetFileHashesRequest => {
                handlers::handle_get_file_hashes(&mut send, payload_buf, &compute, &data_dir)
                    .await?;
            }
            MessageType::ReadBlockRequest => {
                handlers::handle_read_block(&mut send, payload_buf, &compute, &data_dir).await?;
            }
            MessageType::GetStrongHashesRequest => {
                handlers::handle_get_strong_hashes(&mut send, payload_buf, &compute, &data_dir)
                    .await?;
            }
            MessageType::Ping => {
                handlers::handle_ping(&mut send, payload_buf).await?;
            }
            _ => {
                warn!("Unsupported message type: {:?}", msg_type);
                let error_msg = format!("Unsupported message type: {:?}", msg_type);
                let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                send.write_all(&response.encode()).await?;
            }
        }

        if is_ping {
            break;
        }
    }

    send.flush().await?;
    send.finish()?;

    Ok(())
}
