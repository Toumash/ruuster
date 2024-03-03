use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};



use protos::ruuster::Message;
use types::{FanoutExchange, DirectExchange};

pub mod types;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub type ExchangeName = String;
pub type ExchangeType = dyn Exchange + Send + Sync;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<ExchangeType>>>;
pub type ExchangeMetadata = HashMap<String, String>;

pub type QueueMetadata = HashMap<String, String>;

#[derive(PartialEq, Debug)]
pub enum ExchangeKind {
    Fanout,
    Direct
}

#[derive(PartialEq, Debug)]
pub enum ExchangeError {
    BindFail { reason: String },
    EmptyPayloadFail { reason: String },
    NoRouteKey,
    NoMatchingQueue { route_key: String }
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
            ExchangeError::NoMatchingQueue { route_key } => {
                write!(f ,"No matching queue for route key {}", route_key)
            }
            ExchangeError::NoRouteKey => {
                write!(f, "No routing key found")
            }
        }
    }
}

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName, metadata: &QueueMetadata) -> Result<(), ExchangeError>;
    fn get_bound_queue_names(&self) -> HashSet<QueueName>;
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
            ExchangeKind::Direct => Arc::new(RwLock::new(DirectExchange::default()))
        }
    }
}

impl From<i32> for ExchangeKind {
    fn from(value: i32) -> Self {
        match value {
            0 => ExchangeKind::Fanout,
            1 => ExchangeKind::Direct,
            _ => panic!("wrong value for ExchangeKind")
        }
    }
}

impl Into<i32> for ExchangeKind {
    fn into(self) -> i32 {
        match self {
            ExchangeKind::Fanout => 0,
            ExchangeKind::Direct => 1
        }
    }
}