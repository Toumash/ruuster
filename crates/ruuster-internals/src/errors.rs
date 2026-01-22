use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum RuusterError {
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Invalid metadata: {0}")]
    InvalidMetadata(String),

    #[error("Queue full: {0}")]
    QueueFull(String),

    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    #[error("Exchange not found: {0}")]
    ExchangeNotFound(String),

    #[error("Message processing error: {0}")]
    MessageProcessingError(String),
}

impl From<RuusterError> for tonic::Status {
    fn from(err: RuusterError) -> Self {
        match err {
            RuusterError::QueueFull(m) => tonic::Status::resource_exhausted(m),
            RuusterError::InvalidMetadata(m) => tonic::Status::invalid_argument(m),
            _ => tonic::Status::internal("Internal broker error"),
        }
    }
}
