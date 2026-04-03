use crate::util::error::{HarDataError, Result};
use deadpool::managed::{Manager, Pool, RecycleError};
use std::mem::MaybeUninit;
use std::time::Duration;
use tokio::io::Interest;
use tokio::net::TcpStream;
use tracing::{debug, info};

pub struct TcpConnectionManager {
    server_addr: String,
}

impl TcpConnectionManager {
    pub fn new(server_addr: String) -> Self {
        let addr_clean = server_addr
            .strip_prefix("tcp://")
            .unwrap_or(&server_addr)
            .to_string();

        Self {
            server_addr: addr_clean,
        }
    }
}

impl Manager for TcpConnectionManager {
    type Type = TcpStream;
    type Error = HarDataError;

    async fn create(&self) -> std::result::Result<TcpStream, HarDataError> {
        debug!("Creating new TCP connection to {}", self.server_addr);

        use tokio::net::lookup_host;
        let addrs: Vec<_> = lookup_host(&self.server_addr)
            .await
            .map_err(|e| {
                HarDataError::InvalidConfig(format!(
                    "Failed to resolve server address '{}': {}",
                    self.server_addr, e
                ))
            })?
            .collect();

        if addrs.is_empty() {
            return Err(HarDataError::InvalidConfig(format!(
                "No addresses found for server: {}",
                self.server_addr
            )));
        }

        let addr = addrs
            .iter()
            .find(|a| a.is_ipv4())
            .or_else(|| addrs.first())
            .copied()
            .unwrap();

        let stream = TcpStream::connect(addr).await?;

        stream.set_nodelay(true)?;

        let sock_ref = socket2::SockRef::from(&stream);
        sock_ref.set_send_buffer_size(1024 * 1024)?;
        sock_ref.set_recv_buffer_size(1024 * 1024)?;

        let keepalive = socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10));
        sock_ref.set_tcp_keepalive(&keepalive)?;

        info!(
            "TCP pooled connection created: {} (nodelay=true, buffers=1MB, keepalive=60s)",
            addr
        );
        Ok(stream)
    }

    async fn recycle(
        &self,
        conn: &mut TcpStream,
        _metrics: &deadpool::managed::Metrics,
    ) -> std::result::Result<(), RecycleError<Self::Error>> {
        let socket = socket2::SockRef::from(&*conn);
        let mut peek_buf = [MaybeUninit::<u8>::uninit(); 1];
        match conn.try_io(Interest::READABLE, || socket.peek(&mut peek_buf)) {
            Ok(0) => Err(RecycleError::message("Connection closed by peer")),
            Ok(_) => Err(RecycleError::message("Connection has unread data")),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Err(_) => Err(RecycleError::message("Connection error")),
        }
    }
}

pub struct TcpConnectionPool {
    pool: Pool<TcpConnectionManager>,
}

impl TcpConnectionPool {
    pub fn new(server_addr: String, max_size: Option<usize>) -> Result<Self> {
        let manager = TcpConnectionManager::new(server_addr.clone());
        let pool = Pool::builder(manager)
            .max_size(max_size.unwrap_or(10))
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(|e| {
                HarDataError::InvalidConfig(format!("Failed to create TCP connection pool: {}", e))
            })?;

        info!(
            "TCP connection pool created for {} (max_size: {})",
            server_addr,
            max_size.unwrap_or(10)
        );

        Ok(Self { pool })
    }

    pub async fn get(&self) -> Result<deadpool::managed::Object<TcpConnectionManager>> {
        self.pool.get().await.map_err(|e| {
            HarDataError::ConnectionError(format!("Failed to get connection from pool: {}", e))
        })
    }

    pub fn status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            size: status.size,
            available: status.available,
            max_size: status.max_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStatus {
    pub size: usize,
    pub available: usize,
    pub max_size: usize,
}
