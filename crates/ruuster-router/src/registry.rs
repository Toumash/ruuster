use crate::exchange::Exchange;
use crate::strategies::RoutingStrategy;
use dashmap::DashMap;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::sync::Arc;

#[derive(Default)]
pub struct Router {
    /// A thread-safe map of all named exchanges in the system.
    exchanges: DashMap<String, Arc<Exchange>>,
    queues: DashMap<String, Arc<Queue>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            exchanges: DashMap::new(),
            queues: DashMap::new(),
        }
    }

    /// Declare (Create) an exchange with a specific name and strategy.
    /// If an exchange with that name exists, it returns the existing one.
    pub fn add_exchange(
        &self,
        name: &str,
        strategy: Box<dyn RoutingStrategy>,
    ) -> Arc<Exchange> {
        self.exchanges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Exchange::new(name.to_string(), strategy)))
            .clone()
    }

    /// The main entry point for message delivery.
    /// Finds the exchange and delegates the routing to it.
    pub fn route(&self, exchange_name: &str, msg: Message) -> Result<(), RuusterError> {
        let exchange = self.exchanges.get(exchange_name).ok_or_else(|| {
            RuusterError::InvalidMetadata(format!("Exchange '{}' not found", exchange_name))
        })?;

        // This calls the Strategy pattern we built earlier
        exchange.route(msg)
    }

    /// Helper to get an exchange handle (e.g., for binding queues).
    pub fn get_exchange(&self, name: &str) -> Option<Arc<Exchange>> {
        self.exchanges.get(name).map(|r| Arc::clone(&r))
    }

    // Add this method
    pub fn get_queue(&self, name: &str) -> Option<Arc<Queue>> {
        self.queues.get(name).map(|r| Arc::clone(&r))
    }

    // Add a method to register queues (used during setup)
    pub fn add_queue(&self, queue: Arc<Queue>) {
        self.queues.insert(queue.name.clone(), queue);
    }

    pub fn remove_queue(&self, name: &str) -> Result<(), RuusterError> {
        self.queues.remove(name).ok_or_else(|| {
            RuusterError::QueueNotFound(format!("Queue '{}' not found", name))
        })?;
        Ok(())
    }

    pub fn remove_exchange(&self, name: &str) -> Result<(), RuusterError> {
        self.exchanges.remove(name).ok_or_else(|| {
            RuusterError::ExchangeNotFound(format!("Exchange '{}' not found", name))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::direct::DirectStrategy;

    #[test]
    fn test_router_exchange_lifecycle() {
        let router = Router::new();

        // 1. Declare
        router.add_exchange("orders", Box::new(DirectStrategy));

        // 2. Retrieve
        let ex = router.get_exchange("orders");
        assert!(ex.is_some());
        assert_eq!(ex.unwrap().name, "orders");

        // 3. Duplicate declaration shouldn't overwrite or crash
        router.add_exchange("orders", Box::new(DirectStrategy));
        assert!(router.get_exchange("orders").is_some());
    }
}
