use ruuster_internals::RuusterError;
use thiserror::Error;

/// Server-specific errors for the Ruuster message broker
#[derive(Error, Debug, Clone)]
pub enum ServerError {
    /// Queue with the specified name was not found
    #[error("Queue not found: '{0}'")]
    QueueNotFound(String),

    /// Exchange with the specified name was not found
    #[error("Exchange not found: '{0}'")]
    ExchangeNotFound(String),

    /// Consumer with the specified ID was not found
    #[error("Consumer not found: '{0}'")]
    ConsumerNotFound(String),

    /// Queue is full and cannot accept more messages
    #[error("Queue '{queue_name}' is full (capacity: {capacity})")]
    QueueFull { queue_name: String, capacity: u64 },

    /// Consumer has reached prefetch limit
    #[error("Consumer '{consumer_id}' has reached prefetch limit ({current}/{limit})")]
    PrefetchLimitReached {
        consumer_id: String,
        limit: u16,
        current: usize,
    },

    /// Invalid consumer configuration
    #[error("Invalid consumer configuration: {reason}")]
    InvalidConsumerConfig { reason: String },

    /// Invalid queue configuration
    #[error("Invalid queue configuration for '{queue_name}': {reason}")]
    InvalidQueueConfig {
        queue_name: String,
        reason: String,
    },

    /// Invalid exchange configuration
    #[error("Invalid exchange configuration for '{exchange_name}': {reason}")]
    InvalidExchangeConfig {
        exchange_name: String,
        reason: String,
    },

    /// Routing error from the router layer
    #[error("Routing error: {0}")]
    RoutingError(#[from] RuusterError),

    /// Message acknowledgment error
    #[error("Acknowledgment error for message '{message_id}': {reason}")]
    AckError {
        message_id: String,
        reason: String,
    },

    /// Internal server error
    #[error("Internal server error: {0}")]
    Internal(String),
}

// ===== Conversions to tonic::Status for gRPC responses =====

impl From<ServerError> for tonic::Status {
    fn from(err: ServerError) -> Self {
        match err {
            ServerError::QueueNotFound(ref name) => {
                tonic::Status::not_found(format!("Queue '{}' not found", name))
            }
            ServerError::ExchangeNotFound(ref name) => {
                tonic::Status::not_found(format!("Exchange '{}' not found", name))
            }
            ServerError::ConsumerNotFound(ref id) => {
                tonic::Status::not_found(format!("Consumer '{}' not found", id))
            }
            ServerError::QueueFull {
                ref queue_name,
                capacity,
            } => tonic::Status::resource_exhausted(format!(
                "Queue '{}' is full (capacity: {})",
                queue_name, capacity
            )),
            ServerError::PrefetchLimitReached {
                ref consumer_id,
                limit,
                current,
            } => tonic::Status::resource_exhausted(format!(
                "Consumer '{}' has reached prefetch limit ({}/{})",
                consumer_id, current, limit
            )),
            ServerError::InvalidConsumerConfig { ref reason } => {
                tonic::Status::invalid_argument(format!(
                    "Invalid consumer configuration: {}",
                    reason
                ))
            }
            ServerError::InvalidQueueConfig {
                ref queue_name,
                ref reason,
            } => tonic::Status::invalid_argument(format!(
                "Invalid queue configuration for '{}': {}",
                queue_name, reason
            )),
            ServerError::InvalidExchangeConfig {
                ref exchange_name,
                ref reason,
            } => tonic::Status::invalid_argument(format!(
                "Invalid exchange configuration for '{}': {}",
                exchange_name, reason
            )),
            ServerError::RoutingError(ref err) => {
                tonic::Status::internal(format!("Routing error: {}", err))
            }
            ServerError::AckError {
                ref message_id,
                ref reason,
            } => tonic::Status::failed_precondition(format!(
                "Acknowledgment error for message '{}': {}",
                message_id, reason
            )),
            ServerError::Internal(ref msg) => {
                tonic::Status::internal(format!("Internal server error: {}", msg))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ServerError::QueueNotFound("test_queue".to_string());
        assert_eq!(err.to_string(), "Queue not found: 'test_queue'");

        let err = ServerError::QueueFull {
            queue_name: "my_queue".to_string(),
            capacity: 100,
        };
        assert_eq!(
            err.to_string(),
            "Queue 'my_queue' is full (capacity: 100)"
        );

        let err = ServerError::InvalidConsumerConfig {
            reason: "Prefetch count must be > 0".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Invalid consumer configuration: Prefetch count must be > 0"
        );
    }

    #[test]
    fn test_error_conversion_to_tonic_status() {
        let err = ServerError::QueueNotFound("test".to_string());
        let status = tonic::Status::from(err);
        assert_eq!(status.code(), tonic::Code::NotFound);

        let err = ServerError::QueueFull {
            queue_name: "test".to_string(),
            capacity: 10,
        };
        let status = tonic::Status::from(err);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);

        let err = ServerError::InvalidConsumerConfig {
            reason: "test".to_string(),
        };
        let status = tonic::Status::from(err);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_routing_error_conversion() {
        let routing_err = RuusterError::ExchangeNotFound("test_exchange".to_string());
        let server_err = ServerError::from(routing_err);

        match server_err {
            ServerError::RoutingError(_) => (),
            _ => panic!("Expected RoutingError variant"),
        }
    }

    #[test]
    fn test_error_source() {
        use std::error::Error;

        let routing_err = RuusterError::ExchangeNotFound("test".to_string());
        let server_err = ServerError::RoutingError(routing_err);

        // thiserror automatically implements source()
        assert!(server_err.source().is_some());
    }

    #[test]
    fn test_prefetch_limit_error() {
        let err = ServerError::PrefetchLimitReached {
            consumer_id: "abc-123".to_string(),
            limit: 10,
            current: 10,
        };
        assert_eq!(
            err.to_string(),
            "Consumer 'abc-123' has reached prefetch limit (10/10)"
        );
    }

    #[test]
    fn test_ack_error() {
        let err = ServerError::AckError {
            message_id: "msg-123".to_string(),
            reason: "timeout exceeded".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Acknowledgment error for message 'msg-123': timeout exceeded"
        );
    }
}
