use crate::util::error::HarDataError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Transient,
    Retriable,
    Fatal,
    Unknown,
}

pub struct SmartRetryPolicy;

impl SmartRetryPolicy {
    pub fn new() -> Self {
        Self
    }

    pub fn categorize_error(&self, error: &HarDataError) -> ErrorCategory {
        let msg = error.to_string().to_lowercase();

        if msg.contains("timeout") || msg.contains("timed out") {
            return ErrorCategory::Transient;
        }

        match error {
            HarDataError::ConnectionError(_) => ErrorCategory::Retriable,
            HarDataError::NetworkError(_) => ErrorCategory::Retriable,
            HarDataError::Io(_) => ErrorCategory::Retriable,
            HarDataError::QuicConnection(_) => ErrorCategory::Retriable,
            HarDataError::QuicWrite(_) => ErrorCategory::Retriable,
            HarDataError::QuicRead(_) => ErrorCategory::Retriable,

            HarDataError::FileOperation(_) => {
                if msg.contains("not found") || msg.contains("permission denied") {
                    ErrorCategory::Fatal
                } else {
                    ErrorCategory::Retriable
                }
            }
            HarDataError::SerializationError(_) => ErrorCategory::Fatal,
            HarDataError::InvalidProtocol(_) => ErrorCategory::Fatal,
            HarDataError::InvalidConfig(_) => ErrorCategory::Fatal,

            _ => ErrorCategory::Unknown,
        }
    }
}

impl Default for SmartRetryPolicy {
    fn default() -> Self {
        Self
    }
}
