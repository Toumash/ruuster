use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Display};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

use protos::ruuster::Message;
use protos::ruuster::Metadata;
use tracing::{debug, error, warn};
use types::{DirectExchange, FanoutExchange};

pub mod types;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub type ExchangeName = String;
pub type ExchangeType = dyn Exchange + Send + Sync;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<ExchangeType>>>;

#[derive(PartialEq, Debug)]
pub enum ExchangeKind {
    Fanout,
    Direct,
}

#[derive(PartialEq, Debug)]
pub enum ExchangeError {
    BindFail,
    EmptyPayloadFail,
    GetSystemTimeFail {},
    DeadLetterQueueLockFail {},
    NoRouteKey,
    NoMatchingQueue { route_key: String },
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeError::BindFail => {
                write!(f, "binding to exchange failed")
            }
            ExchangeError::EmptyPayloadFail => {
                write!(f, "handling message failed")
            }
            ExchangeError::GetSystemTimeFail {} => {
                write!(
                    f,
                    "handling message failed: SystemTime::now().duration_since"
                )
            }
            ExchangeError::DeadLetterQueueLockFail {} => write!(
                f,
                "handling message failed: queue().lock() failed for dead letter queue"
            ),

            ExchangeError::NoMatchingQueue { route_key } => {
                write!(f, "No matching queue for route key {}", route_key)
            }
            ExchangeError::NoRouteKey => {
                write!(f, "No routing key found")
            }
        }
    }
}

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName, metadata: Option<&Metadata>) -> Result<(), ExchangeError>;
    fn get_bound_queue_names(&self) -> HashSet<QueueName>;
    fn handle_message(
        &self,
        message: Message,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError>;
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
            ExchangeKind::Fanout => write!(f, "{}", "ExchangeKind::Fanout"),
            ExchangeKind::Direct => write!(f, "{}", "ExchangeKind::Direct"),
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
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .map_err(|_| ExchangeError::GetSystemTimeFail {})?;

        let queue_lock = &mut queue.lock().unwrap();
        if queue_lock.len() >= QUEUE_MAX_LENGTH {
            warn!("queue size reached for queue {}", name);
            if let Some(dead_letter_queue) = queues_read.get(DEADLETTER_QUEUE_NAME) {
                // FIXME: use the deadletter queue defined per exchange
                debug!("moving the message {} to the dead letter queue", message.uuid);

                // FIXME: convert to ruuster headers
                let val = json!({
                    "count": 1,
                    "exchange": exchange_name,
                    "original_message": message.payload,
                    "reason": "max_len",
                    "time": timestamp,
                    "queue": name.to_string(),
                })
                .to_string();

                dead_letter_queue
                    .lock()
                    .map_err(|_| ExchangeError::DeadLetterQueueLockFail {})?
                    .push_back(Message {
                        uuid: message.uuid,
                        payload: val,
                        metadata: None
                    });
            } else {
                debug!("message {} dropped", message.uuid);
            }
            return Ok(PushResult::QueueOverflow);
        } else {
            queue_lock.push_back(message);
            return Ok(PushResult::Ok);
        }
    }
}
