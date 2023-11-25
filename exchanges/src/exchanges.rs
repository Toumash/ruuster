use std::fmt;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::{HashMap, VecDeque, HashSet};

use protos::ruuster::Message;

pub mod types;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub type ExchangeName = String;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<dyn Exchange + Send + Sync>>>;

#[derive(PartialEq, Debug)]
pub enum ExchangeError {
    BindFail { reason: String },
    HandleMessageFail { reason: String },
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeError::BindFail { reason } => write!(f, "binding to exchange failed: {}", reason),
            ExchangeError::HandleMessageFail { reason } => write!(f, "handling message failed: {}", reason)
        }
    }
}

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName) -> Result<(), ExchangeError>;
    fn get_bound_queue_names(&self) -> &HashSet<QueueName>;
    fn handle_message(&self, message: &Message, queues: Arc<RwLock<QueueContainer>>) -> Result<(), ExchangeError>;
}

