use crate::agent::compute::ComputeService;
use crate::agent::server::common::{
    MAX_CONCURRENT_CONNECTIONS, MAX_PAYLOAD_SIZE, REQUEST_TIMEOUT_SECS,
};
use crate::core::protocol::{ISyncMessage, MessageType};
use crate::util::error::{HarDataError, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};

use super::handlers;

pub struct TcpServer {
    listener: TcpListener,
    compute: Arc<ComputeService>,
    data_dir: PathBuf,
    connection_semaphore: Arc<Semaphore>,
    active_connections: Arc<AtomicUsize>,
}

impl TcpServer {
    pub async fn new(
        bind_addr: &str,
        compute: Arc<ComputeService>,
        data_dir: &str,
    ) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr).await?;
        info!("TCP Server listening on {}", bind_addr);

        Ok(Self {
            listener,
            compute,
            data_dir: PathBuf::from(data_dir),
            connection_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "TCP Server started with max_connections={}",
            MAX_CONCURRENT_CONNECTIONS
        );

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let permit = match self.connection_semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            warn!(
                                "TCP connection limit reached ({}), rejecting connection from {}",
                                MAX_CONCURRENT_CONNECTIONS, addr
                            );
                            continue;
                        }
                    };

                    info!("New TCP connection from: {}", addr);

                    if let Err(e) = stream.set_nodelay(true) {
                        tracing::warn!("Failed to set TCP_NODELAY for {}: {}", addr, e);
                    }

                    let sock_ref = socket2::SockRef::from(&stream);
                    if let Err(e) = sock_ref.set_send_buffer_size(1024 * 1024) {
                        tracing::warn!("Failed to set send buffer for {}: {}", addr, e);
                    }
                    if let Err(e) = sock_ref.set_recv_buffer_size(1024 * 1024) {
                        tracing::warn!("Failed to set recv buffer for {}: {}", addr, e);
                    }

                    info!("TCP optimizations applied: nodelay=true, buffers=1MB");

                    let compute = self.compute.clone();
                    let data_dir = self.data_dir.clone();
                    let active_connections = self.active_connections.clone();

                    active_connections.fetch_add(1, Ordering::Relaxed);

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, compute, data_dir).await {
                            error!("TCP connection error from {}: {}", addr, e);
                        }
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                    });
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                }
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    compute: Arc<ComputeService>,
    data_dir: PathBuf,
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    let request_timeout = Duration::from_secs(REQUEST_TIMEOUT_SECS);

    loop {
        let mut header_buf = [0u8; ISyncMessage::HEADER_SIZE];
        let header_read_result = match timeout(request_timeout, stream.read_exact(&mut header_buf))
            .await
        {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    "TCP request timeout after {} seconds from {}",
                    REQUEST_TIMEOUT_SECS, peer_addr
                );
                let error_msg = format!("Request timeout after {} seconds", REQUEST_TIMEOUT_SECS);
                let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
                let _ = stream.write_all(&response.encode()).await;
                return Err(HarDataError::NetworkError("Request timeout".to_string()));
            }
        };

        match header_read_result {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("TCP connection closed by peer: {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("Failed to read header from {}: {}", peer_addr, e);
                break;
            }
        }

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;

        if payload_len as usize > MAX_PAYLOAD_SIZE {
            error!(
                "TCP payload size {} exceeds limit {} from {}",
                payload_len, MAX_PAYLOAD_SIZE, peer_addr
            );
            let error_msg = format!(
                "Payload size {} exceeds maximum allowed size {}",
                payload_len, MAX_PAYLOAD_SIZE
            );
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            stream.write_all(&response.encode()).await?;
            continue;
        }

        info!(
            "Received TCP message from {}: type={:?}, payload_len={}",
            peer_addr, msg_type, payload_len
        );

        use crate::util::buffer_pool::global_buffer_pool;
        let pooled_buffer = if payload_len > 0 {
            let pool = global_buffer_pool();
            let mut buffer = pool.acquire();
            buffer.resize(payload_len as usize, 0);
            stream
                .read_exact(&mut buffer[..payload_len as usize])
                .await?;
            Some(buffer)
        } else {
            None
        };

        let payload_buf: &[u8] = match &pooled_buffer {
            Some(buf) => &buf[..payload_len as usize],
            None => &[],
        };

        let response = handlers::handle_message(msg_type, payload_buf, &compute, &data_dir).await;

        stream.write_all(&response.encode()).await?;
    }

    Ok(())
}
