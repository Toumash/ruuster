use std::sync::{RwLock, Arc, Mutex};
use std::collections::{HashMap, VecDeque};

use protos::ruuster::Message;

pub mod types;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub type ExchangeName = String;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<dyn Exchange + Send + Sync>>>;

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName);
    fn get_bound_queue_names(&self) -> &[QueueName];
    fn handle_message(&self, message: &Message, queues: Arc<RwLock<QueueContainer>>);
}

