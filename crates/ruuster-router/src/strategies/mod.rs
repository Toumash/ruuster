pub mod direct;
pub mod fanout;

use dashmap::DashSet;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::sync::Arc;

pub trait RoutingStrategy: Send + Sync {
    fn route(&self, msg: Message, bindings: &DashSet<Arc<Queue>>) -> Result<(), RuusterError>;
}
