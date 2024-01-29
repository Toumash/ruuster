use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

use protos::ruuster::Message;

pub mod types;

pub type QueueName = String;
pub type Queue = VecDeque<Message>;
pub type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub type ExchangeName = String;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<dyn Exchange + Send + Sync>>>;

#[derive(PartialEq, Debug)]
pub enum ExchangeError {
    BindFail { reason: String },
    EmptyPayloadFail { reason: String },
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
        }
    }
}

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName) -> Result<(), ExchangeError>;
    fn get_bound_queue_names(&self) -> &HashSet<QueueName>;
    fn handle_message(
        &self,
        message: &Option<Message>,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError>;
}
