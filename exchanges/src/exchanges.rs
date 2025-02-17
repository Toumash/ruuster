use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Display};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use internals::{DeadLetterMetadata, Message, Metadata};
use serde::{Deserialize, Serialize};

use tracing::{debug, error, info, warn};
use types::{DirectExchange, FanoutExchange};

pub mod types;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub type ExchangeName = String;
pub type ExchangeType = dyn Exchange + Send + Sync;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<ExchangeType>>>;
pub type MessageCount = u32;

#[derive(PartialEq, Debug)]
pub enum ExchangeKind {
    Fanout,
    Direct,
}

#[derive(PartialEq, Debug)]
pub enum ExchangeError {
    BindFail,
    EmptyPayloadFail,
    DeadLetterQueueLockFail,
    RoutingKeyNotFound,
    MessageWithoutMetadata,
    MissingRoutingKey,
    RoutingSetNotFound,
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeError::BindFail => {
                write!(f, "binding to exchange failed")
            }
            ExchangeError::EmptyPayloadFail => {
                write!(f, "handling message failed: empty payload")
            }
            ExchangeError::DeadLetterQueueLockFail {} => write!(
                f,
                "handling message failed: queue().lock() failed for dead letter queue"
            ),
            ExchangeError::MessageWithoutMetadata => write!(
                f,
                "handling message failed: insufficient metadata in message"
            ),
            ExchangeError::RoutingKeyNotFound => {
                write!(f, "No routing key found")
            }
            ExchangeError::MissingRoutingKey => {
                write!(f, "Routing key is required for this operation")
            }
            ExchangeError::RoutingSetNotFound => {
                write!(f, "There is none routing set assigned to this queue")
            }
        }
    }
}

pub trait Exchange {
    fn bind(
        &mut self,
        queue_name: &QueueName,
        metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), ExchangeError>;

    fn unbind(
        &mut self,
        queue_name: &QueueName,
        metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), ExchangeError>;

    fn get_bound_queue_names(&self) -> HashSet<QueueName>;

    fn get_bind_count(&self) -> u32;

    fn handle_message(
        &self,
        message: Message,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<MessageCount, ExchangeError>;
}

impl ExchangeKind {
    // exchanges factory
    pub fn create(&self) -> Arc<RwLock<ExchangeType>> {
        match self {
            ExchangeKind::Fanout => Arc::new(RwLock::new(FanoutExchange::default())),
            ExchangeKind::Direct => Arc::new(RwLock::new(DirectExchange::default())),
        }
    }
}

impl From<i32> for ExchangeKind {
    fn from(value: i32) -> Self {
        match value {
            0 => ExchangeKind::Fanout,
            1 => ExchangeKind::Direct,
            wrong_value => {
                error!(
                    "value {} is not correct ExchangeKind, will use Fanout",
                    wrong_value
                );
                ExchangeKind::Fanout
            }
        }
    }
}

impl From<ExchangeKind> for i32 {
    fn from(value: ExchangeKind) -> Self {
        match value {
            ExchangeKind::Fanout => 0,
            ExchangeKind::Direct => 1,
        }
    }
}

impl Display for ExchangeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ExchangeKind::Fanout => write!(f, "ExchangeKind::Fanout"),
            ExchangeKind::Direct => write!(f, "ExchangeKind::Direct"),
        }
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum PushResult {
    Ok,
    QueueOverflow,
}
pub const QUEUE_MAX_LENGTH: usize = 1_000;
pub const DEADLETTER_QUEUE_NAME: &str = "_deadletter";
pub(crate) trait PushToQueueStrategy {
    fn push_to_queue(
        &self,
        exchange_name: &String,
        message: Message,
        queue: &Arc<Mutex<VecDeque<Message>>>,
        name: &String,
        queues_read: &std::sync::RwLockReadGuard<
            '_,
            HashMap<String, Arc<Mutex<VecDeque<Message>>>>,
        >,
    ) -> Result<PushResult, ExchangeError> {
        let queue_lock = &mut queue.lock().unwrap();
        if queue_lock.len() >= QUEUE_MAX_LENGTH {
            warn!("queue size reached for queue {}", name);
            if let Some(dead_letter_queue) = queues_read.get(DEADLETTER_QUEUE_NAME) {
                // TODO: use the deadletter queue defined per exchange
                debug!(
                    "moving the message {} to the dead letter queue",
                    message.uuid
                );
                let mut meta = message.metadata.unwrap_or(Metadata {
                    created_at: Some(Instant::now()),
                    routing_key: None,
                    dead_letter: None,
                });
                let deadletter_metadata = DeadLetterMetadata {
                    count: Some(1),
                    exchange: Some(exchange_name.to_string()),
                    queue: Some(name.to_string()),
                };
                meta.dead_letter = Some(deadletter_metadata);

                let message = Message {
                    uuid: message.uuid,
                    payload: message.payload,
                    metadata: Some(meta),
                };

                dead_letter_queue
                    .lock()
                    .map_err(|_| ExchangeError::DeadLetterQueueLockFail {})?
                    .push_back(message);
            } else {
                info!("message {} dropped", message.uuid);
            }
            Ok(PushResult::QueueOverflow)
        } else {
            queue_lock.push_back(message);
            Ok(PushResult::Ok)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DeadLetterMessage {
    count: i32,
    exchange: String,
    original_message: String,
    time: i64,
    queue: String,
}
