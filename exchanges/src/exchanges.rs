use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

use protos::ruuster::Message;
use types::FanoutExchange;

pub mod types;

pub type QueueName = String;
pub type Queue = VecDeque<Message>;
pub type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub type ExchangeName = String;
pub type ExchangeType = dyn Exchange + Send + Sync;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<ExchangeType>>>;

#[derive(PartialEq, Debug)]
pub enum ExchangeKind {
    Fanout
}

#[derive(PartialEq, Debug)]
pub enum ExchangeError {
    BindFail { reason: String },
    EmptyPayloadFail { reason: String },
    GetSystemTimeFail {},
    DeadLetterQueueLockFail {},
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeError::BindFail { reason } => {
                write!(f, "binding to exchange failed: {}", reason)
            }
            ExchangeError::EmptyPayloadFail { reason } => {
                write!(f, "handling message failed: {}", reason)
            }
            ExchangeError::GetSystemTimeFail {} => {
                write!(
                    f,
                    "handling message failed: SystemTime::now().duration_since"
                )
            }
            ExchangeError::DeadLetterQueueLockFail {} => write!(f, "handling message failed: queue().lock() failed for dead letter queue"),
        }
    }
}

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName) -> Result<(), ExchangeError>;
    fn get_bound_queue_names(&self) -> Vec<QueueName>;
    fn handle_message(
        &self,
        message: &Option<Message>,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError>;
}

impl ExchangeKind {
    // exchanges factory
    pub fn create(&self) -> Arc<RwLock<ExchangeType>> {
        match self {
            ExchangeKind::Fanout => Arc::new(RwLock::new(FanoutExchange::default())),
        }
    }
}

impl From<i32> for ExchangeKind {
    fn from(value: i32) -> Self {
        match value {
            0 => ExchangeKind::Fanout,
            wrong_value => {
                log::error!("value {} is not correct ExchangeKind, will use Fanout", wrong_value);
                ExchangeKind::Fanout
            }
        }
    }
}

impl Into<i32> for ExchangeKind {
    fn into(self) -> i32 {
        match self {
            ExchangeKind::Fanout => 0,
        }
    }
}