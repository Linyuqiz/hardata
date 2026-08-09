use crate::agent_server::common::{
    MAX_CONCURRENT_CONNECTIONS, MAX_PAYLOAD_SIZE, REQUEST_TIMEOUT_SECS,
};
use crate::compute::ComputeService;
use hardata_protocol::{ISyncMessage, MessageType};
use hardata_shared::error::{HarDataError, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};

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
            operation = "agent.tcp_ready",
            max_connections = MAX_CONCURRENT_CONNECTIONS,
            "TCP server listening"
        );

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let permit = match self.connection_semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            warn!(
                                operation = "agent.tcp_connection_rejected",
                                peer = %addr,
                                max_connections = MAX_CONCURRENT_CONNECTIONS,
                                "TCP connection rejected at capacity"
                            );
                            continue;
                        }
                    };

                    debug!(operation = "agent.tcp_connection_accepted", peer = %addr, "TCP connection accepted");

                    if let Err(e) = stream.set_nodelay(true) {
                        warn!(operation = "agent.tcp_socket_option_failed", peer = %addr, option = "tcp_nodelay", error = %e, "TCP socket option failed");
                    }

                    let sock_ref = socket2::SockRef::from(&stream);
                    if let Err(e) = sock_ref.set_send_buffer_size(1024 * 1024) {
                        warn!(operation = "agent.tcp_socket_option_failed", peer = %addr, option = "send_buffer", error = %e, "TCP socket option failed");
                    }
                    if let Err(e) = sock_ref.set_recv_buffer_size(1024 * 1024) {
                        warn!(operation = "agent.tcp_socket_option_failed", peer = %addr, option = "recv_buffer", error = %e, "TCP socket option failed");
                    }

                    let compute = self.compute.clone();
                    let data_dir = self.data_dir.clone();
                    let active_connections = self.active_connections.clone();

                    active_connections.fetch_add(1, Ordering::Relaxed);

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, compute, data_dir).await {
                            error!(operation = "agent.tcp_connection_failed", peer = %addr, error = %e, "TCP connection failed");
                        }
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                    });
                }
                Err(e) => {
                    error!(operation = "agent.tcp_accept_failed", error = %e, "TCP connection accept failed");
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
                    operation = "agent.tcp_request_timeout",
                    peer = %peer_addr,
                    timeout_secs = REQUEST_TIMEOUT_SECS,
                    "TCP request timed out"
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
                debug!(operation = "agent.tcp_connection_closed", peer = %peer_addr, "TCP connection closed by peer");
                break;
            }
            Err(e) => {
                error!(operation = "agent.tcp_header_read_failed", peer = %peer_addr, error = %e, "TCP request header read failed");
                break;
            }
        }

        let (msg_type, payload_len) = ISyncMessage::decode_header(&header_buf)?;

        if payload_len as usize > MAX_PAYLOAD_SIZE {
            error!(
                operation = "agent.tcp_payload_rejected",
                peer = %peer_addr,
                payload_len,
                max_payload_size = MAX_PAYLOAD_SIZE,
                "TCP payload exceeds configured limit"
            );
            let error_msg = format!(
                "Payload size {} exceeds maximum allowed size {}",
                payload_len, MAX_PAYLOAD_SIZE
            );
            let response = ISyncMessage::new(MessageType::Error, bytes::Bytes::from(error_msg));
            stream.write_all(&response.encode()).await?;
            continue;
        }

        debug!(
            operation = "agent.tcp_message_received",
            peer = %peer_addr,
            message_type = ?msg_type,
            payload_len,
            "TCP message received"
        );

        use hardata_shared::buffer_pool::global_buffer_pool;
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
