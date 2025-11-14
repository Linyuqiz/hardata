use thiserror::Error;

#[derive(Error, Debug)]
pub enum HarDataError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("QUIC connection error: {0}")]
    QuicConnection(#[from] quinn::ConnectionError),

    #[error("QUIC write error: {0}")]
    QuicWrite(#[from] quinn::WriteError),

    #[error("QUIC read error: {0}")]
    QuicRead(#[from] quinn::ReadError),

    #[error("QUIC read exact error: {0}")]
    QuicReadExact(#[from] quinn::ReadExactError),

    #[error("QUIC connect error: {0}")]
    QuicConnect(#[from] quinn::ConnectError),

    #[error("QUIC closed stream: {0}")]
    QuicClosedStream(#[from] quinn::ClosedStream),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("HTTP request error: {0}")]
    HttpRequest(#[from] reqwest::Error),

    #[error("Job not found: {0}")]
    JobNotFound(String),

    #[error("Invalid protocol message: {0}")]
    InvalidProtocol(String),

    #[error("File operation error: {0}")]
    FileOperation(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),
}

pub type Result<T> = std::result::Result<T, HarDataError>;
