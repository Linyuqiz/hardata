use crate::sync::net::quic::QuicClient;
use crate::sync::net::tcp::TcpClient;
use crate::sync::transfer::batch::{
    BatchTransferItem, BatchTransferResult, CancelCallback, ProgressCallback,
};
use crate::util::error::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Quic,
    Tcp,
}

pub struct ProtocolSelector {
    quic_success: AtomicU64,
    quic_total: AtomicU64,
    tcp_success: AtomicU64,
    tcp_total: AtomicU64,
}

impl ProtocolSelector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            quic_success: AtomicU64::new(0),
            quic_total: AtomicU64::new(0),
            tcp_success: AtomicU64::new(0),
            tcp_total: AtomicU64::new(0),
        })
    }

    pub fn record_result(&self, protocol: Protocol, success: bool) {
        match protocol {
            Protocol::Quic => {
                self.quic_total.fetch_add(1, Ordering::Relaxed);
                if success {
                    self.quic_success.fetch_add(1, Ordering::Relaxed);
                }
            }
            Protocol::Tcp => {
                self.tcp_total.fetch_add(1, Ordering::Relaxed);
                if success {
                    self.tcp_success.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl Default for ProtocolSelector {
    fn default() -> Self {
        Self {
            quic_success: AtomicU64::new(0),
            quic_total: AtomicU64::new(0),
            tcp_success: AtomicU64::new(0),
            tcp_total: AtomicU64::new(0),
        }
    }
}

pub enum TransportConnection {
    Quic {
        client: QuicClient,
        connection: quinn::Connection,
    },
    Tcp {
        client: TcpClient,
    },
}

impl TransportConnection {
    pub async fn read_and_write_batch(
        &mut self,
        items: Vec<BatchTransferItem>,
        job_id: &str,
        max_concurrent_streams: usize,
    ) -> Result<BatchTransferResult> {
        self.read_and_write_batch_with_progress(items, job_id, max_concurrent_streams, None, None)
            .await
    }

    pub async fn read_and_write_batch_with_progress(
        &mut self,
        items: Vec<BatchTransferItem>,
        job_id: &str,
        max_concurrent_streams: usize,
        progress_callback: Option<ProgressCallback>,
        cancel_callback: Option<CancelCallback>,
    ) -> Result<BatchTransferResult> {
        match self {
            TransportConnection::Quic { client, connection } => {
                client
                    .read_and_write_batch_concurrent_with_progress(
                        connection,
                        items,
                        job_id,
                        max_concurrent_streams,
                        progress_callback,
                        cancel_callback,
                    )
                    .await
            }
            TransportConnection::Tcp { client } => {
                client
                    .read_and_write_batch_concurrent_with_progress(
                        items,
                        job_id,
                        max_concurrent_streams,
                        progress_callback,
                        cancel_callback,
                    )
                    .await
            }
        }
    }

    pub async fn list_directory(
        &mut self,
        directory_path: &str,
    ) -> Result<crate::core::protocol::ListDirectoryResponse> {
        match self {
            TransportConnection::Quic { client, connection } => {
                client.list_directory(connection, directory_path).await
            }
            TransportConnection::Tcp { client } => {
                let mut stream = client.get_pooled_connection().await?;
                client.list_directory(&mut stream, directory_path).await
            }
        }
    }

    pub async fn get_strong_hashes(
        &mut self,
        file_path: &str,
        chunks: Vec<crate::core::ChunkLocation>,
    ) -> Result<crate::core::protocol::GetStrongHashesResponse> {
        match self {
            TransportConnection::Quic { client, connection } => {
                client
                    .get_strong_hashes(connection, file_path, chunks)
                    .await
            }
            TransportConnection::Tcp { client } => {
                let mut stream = client.get_pooled_connection().await?;
                client
                    .get_strong_hashes(&mut stream, file_path, chunks)
                    .await
            }
        }
    }
}
