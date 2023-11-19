use std::sync::{Mutex, Arc};
use std::collections::HashMap;

pub mod types;

pub type QueueName = String;

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName);
}

pub type ExchangeName = String;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<Mutex<dyn Exchange + Send + Sync>>>;
