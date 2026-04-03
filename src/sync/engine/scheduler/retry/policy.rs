use crate::util::error::HarDataError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Transient,
    Retriable,
    Fatal,
    Unknown,
}

pub struct SmartRetryPolicy;

fn is_retriable_message(msg: &str) -> bool {
    [
        "not connected to server",
        "connection refused",
        "connection reset",
        "broken pipe",
        "connection aborted",
        "network is unreachable",
        "no route to host",
        "unhealthy",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

impl SmartRetryPolicy {
    pub fn new() -> Self {
        Self
    }

    pub fn categorize_error(&self, error: &HarDataError) -> ErrorCategory {
        let msg = error.to_string().to_lowercase();

        if msg.contains("[fatal]") {
            return ErrorCategory::Fatal;
        }

        if msg.contains("timeout") || msg.contains("timed out") {
            return ErrorCategory::Transient;
        }

        if is_retriable_message(&msg) {
            return ErrorCategory::Retriable;
        }

        match error {
            HarDataError::ConnectionError(_) => ErrorCategory::Retriable,
            HarDataError::NetworkError(_) => ErrorCategory::Retriable,
            HarDataError::Io(_) => ErrorCategory::Retriable,
            HarDataError::QuicConnection(_) => ErrorCategory::Retriable,
            HarDataError::QuicWrite(_) => ErrorCategory::Retriable,
            HarDataError::QuicRead(_) => ErrorCategory::Retriable,
            HarDataError::ProtocolError(_) => ErrorCategory::Fatal,

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

#[cfg(test)]
mod tests {
    use super::{ErrorCategory, SmartRetryPolicy};
    use crate::util::error::HarDataError;

    #[test]
    fn categorize_protocol_not_connected_as_retriable() {
        let policy = SmartRetryPolicy::new();
        let category = policy.categorize_error(&HarDataError::ProtocolError(
            "Not connected to server".to_string(),
        ));
        assert_eq!(category, ErrorCategory::Retriable);
    }

    #[test]
    fn categorize_fatal_marker_as_fatal() {
        let policy = SmartRetryPolicy::new();
        let category = policy.categorize_error(&HarDataError::Unknown(
            "[fatal] 2 files failed: permission denied".to_string(),
        ));
        assert_eq!(category, ErrorCategory::Fatal);
    }
}
