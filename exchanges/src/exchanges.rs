use std::sync::{RwLock, Arc};
use std::collections::HashMap;

pub mod types;

pub type QueueName = String;

pub trait Exchange {
    fn bind(&mut self, queue_name: &QueueName);
    fn get_bound_queue_names(&self) -> &[QueueName];
}

pub type ExchangeName = String;
pub type ExchangeContainer = HashMap<ExchangeName, Arc<RwLock<dyn Exchange + Send + Sync>>>;
