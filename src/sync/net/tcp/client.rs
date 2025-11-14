use crate::sync::net::pool::TcpConnectionPool;
use crate::util::error::{HarDataError, Result};
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::debug;

#[derive(Clone)]
pub struct TcpClient {
    pub(super) server_addr: String,
    pub(super) pool: Option<Arc<TcpConnectionPool>>,
}

impl TcpClient {
    pub fn new(server_addr: String) -> Result<Self> {
        let addr_clean = server_addr
            .strip_prefix("tcp://")
            .unwrap_or(&server_addr)
            .to_string();

        debug!("TCP client created: {}", addr_clean);

        Ok(Self {
            server_addr: addr_clean,
            pool: None,
        })
    }

    pub fn with_pool(server_addr: String, pool_size: Option<usize>) -> Result<Self> {
        let addr_clean = server_addr
            .strip_prefix("tcp://")
            .unwrap_or(&server_addr)
            .to_string();

        let pool = TcpConnectionPool::new(addr_clean.clone(), pool_size)?;

        debug!(
            "TCP client with pool: {}, size={}",
            addr_clean,
            pool_size.unwrap_or(10)
        );

        Ok(Self {
            server_addr: addr_clean,
            pool: Some(Arc::new(pool)),
        })
    }

    pub fn pool_status(&self) -> Option<crate::sync::net::pool::PoolStatus> {
        self.pool.as_ref().map(|p| p.status())
    }

    pub async fn connect(&self) -> Result<TcpStream> {
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
            .ok_or_else(|| {
                HarDataError::InvalidConfig(format!(
                    "Failed to resolve server address (no valid addresses): {}",
                    self.server_addr
                ))
            })?;

        let stream = TcpStream::connect(addr).await?;

        stream.set_nodelay(true)?;

        let sock_ref = socket2::SockRef::from(&stream);
        sock_ref.set_send_buffer_size(1024 * 1024)?;
        sock_ref.set_recv_buffer_size(1024 * 1024)?;

        let keepalive = socket2::TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(60))
            .with_interval(std::time::Duration::from_secs(10));
        sock_ref.set_tcp_keepalive(&keepalive)?;

        debug!("TCP connected: {}", addr);
        Ok(stream)
    }

    pub async fn get_pooled_connection(&self) -> Result<PooledTcpConnection> {
        if let Some(ref pool) = self.pool {
            let conn = pool.get().await?;
            Ok(PooledTcpConnection::Pooled(conn))
        } else {
            let stream = self.connect().await?;
            Ok(PooledTcpConnection::Direct(stream))
        }
    }
}

pub enum PooledTcpConnection {
    Pooled(deadpool::managed::Object<crate::sync::net::pool::TcpConnectionManager>),
    Direct(TcpStream),
}

impl std::ops::Deref for PooledTcpConnection {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        match self {
            PooledTcpConnection::Pooled(obj) => obj,
            PooledTcpConnection::Direct(stream) => stream,
        }
    }
}

impl std::ops::DerefMut for PooledTcpConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            PooledTcpConnection::Pooled(obj) => &mut *obj,
            PooledTcpConnection::Direct(stream) => stream,
        }
    }
}
