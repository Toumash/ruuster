use ruuster_core::Queue;
use ruuster_router::strategies::RoutingStrategy;
use ruuster_router::{DirectStrategy, Router};
use std::sync::Arc;

use crate::error::ServerError;

/// Manages exchange and queue lifecycle operations
pub struct TopologyManager {
    router: Arc<Router>,
}

impl TopologyManager {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }

    /// Add a new queue to the broker
    pub fn add_queue(&self, queue_name: String, max_capacity: u64) -> Result<(), ServerError> {
        let queue = Arc::new(Queue::new(queue_name.clone(), max_capacity));
        self.router.add_queue(queue);
        Ok(())
    }

    /// Remove a queue from the broker
    pub fn remove_queue(&self, queue_name: &str) -> Result<(), ServerError> {
        self.router
            .remove_queue(queue_name)
            .map_err(ServerError::from)
    }

    /// Add a new exchange to the broker
    /// For now, we only support DirectStrategy as the default
    /// TODO: Add support for strategy selection from proto
    pub fn add_exchange(
        &self,
        exchange_name: String,
        strategy: Option<Box<dyn RoutingStrategy>>,
    ) -> Result<(), ServerError> {
        let strategy = strategy.unwrap_or_else(|| Box::new(DirectStrategy));
        self.router.add_exchange(&exchange_name, strategy);
        Ok(())
    }

    /// Remove an exchange from the broker
    pub fn remove_exchange(&self, exchange_name: &str) -> Result<(), ServerError> {
        self.router
            .remove_exchange(exchange_name)
            .map_err(ServerError::from)
    }

    /// Bind a queue to an exchange
    pub fn bind_queue(&self, exchange_name: &str, queue_name: &str) -> Result<(), ServerError> {
        let exchange = self
            .router
            .get_exchange(exchange_name)
            .ok_or_else(|| ServerError::ExchangeNotFound(exchange_name.to_string()))?;

        let queue = self
            .router
            .get_queue(queue_name)
            .ok_or_else(|| ServerError::QueueNotFound(queue_name.to_string()))?;

        exchange.bind(queue);
        Ok(())
    }

    /// Unbind a queue from an exchange
    pub fn unbind_queue(&self, exchange_name: &str, queue_name: &str) -> Result<(), ServerError> {
        let exchange = self
            .router
            .get_exchange(exchange_name)
            .ok_or_else(|| ServerError::ExchangeNotFound(exchange_name.to_string()))?;

        let queue = self
            .router
            .get_queue(queue_name)
            .ok_or_else(|| ServerError::QueueNotFound(queue_name.to_string()))?;

        exchange.unbind(&queue.name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruuster_router::Router;

    fn setup_topology_manager() -> TopologyManager {
        let router = Arc::new(Router::new());
        TopologyManager::new(router)
    }

    #[test]
    fn test_add_queue() {
        let manager = setup_topology_manager();

        let result = manager.add_queue("test_queue".to_string(), 100);
        assert!(result.is_ok());

        // Verify queue was added
        let queue = manager.router.get_queue("test_queue");
        assert!(queue.is_some());
        assert_eq!(queue.unwrap().name, "test_queue");
    }

    #[test]
    fn test_remove_queue() {
        let manager = setup_topology_manager();

        // Add a queue first
        manager.add_queue("test_queue".to_string(), 100).unwrap();

        // Remove it
        let result = manager.remove_queue("test_queue");
        assert!(result.is_ok());

        // Verify it's gone
        assert!(manager.router.get_queue("test_queue").is_none());
    }

    #[test]
    fn test_remove_nonexistent_queue() {
        let manager = setup_topology_manager();

        let result = manager.remove_queue("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_add_exchange() {
        let manager = setup_topology_manager();

        let result = manager.add_exchange("test_exchange".to_string(), None);
        assert!(result.is_ok());

        // Verify exchange was added
        let exchange = manager.router.get_exchange("test_exchange");
        assert!(exchange.is_some());
    }

    #[test]
    fn test_remove_exchange() {
        let manager = setup_topology_manager();

        // Add an exchange first
        manager
            .add_exchange("test_exchange".to_string(), None)
            .unwrap();

        // Remove it
        let result = manager.remove_exchange("test_exchange");
        assert!(result.is_ok());

        // Verify it's gone
        assert!(manager.router.get_exchange("test_exchange").is_none());
    }

    #[test]
    fn test_bind_queue_to_exchange() {
        let manager = setup_topology_manager();

        // Setup: add queue and exchange
        manager.add_queue("test_queue".to_string(), 100).unwrap();
        manager
            .add_exchange("test_exchange".to_string(), None)
            .unwrap();

        // Bind them
        let result = manager.bind_queue("test_exchange", "test_queue");
        assert!(result.is_ok());
    }

    #[test]
    fn test_bind_queue_exchange_not_found() {
        let manager = setup_topology_manager();

        manager.add_queue("test_queue".to_string(), 100).unwrap();

        let result = manager.bind_queue("nonexistent_exchange", "test_queue");
        assert!(result.is_err());

        match result.unwrap_err() {
            ServerError::ExchangeNotFound(name) => {
                assert_eq!(name, "nonexistent_exchange");
            }
            _ => panic!("Expected ExchangeNotFound error"),
        }
    }

    #[test]
    fn test_bind_queue_queue_not_found() {
        let manager = setup_topology_manager();

        manager
            .add_exchange("test_exchange".to_string(), None)
            .unwrap();

        let result = manager.bind_queue("test_exchange", "nonexistent_queue");
        assert!(result.is_err());

        match result.unwrap_err() {
            ServerError::QueueNotFound(name) => {
                assert_eq!(name, "nonexistent_queue");
            }
            _ => panic!("Expected QueueNotFound error"),
        }
    }

    #[test]
    fn test_unbind_queue_from_exchange() {
        let manager = setup_topology_manager();

        // Setup: add queue and exchange, then bind
        manager.add_queue("test_queue".to_string(), 100).unwrap();
        manager
            .add_exchange("test_exchange".to_string(), None)
            .unwrap();
        manager.bind_queue("test_exchange", "test_queue").unwrap();

        // Unbind them
        let result = manager.unbind_queue("test_exchange", "test_queue");
        assert!(result.is_ok());
    }

    #[test]
    fn test_unbind_queue_exchange_not_found() {
        let manager = setup_topology_manager();

        manager.add_queue("test_queue".to_string(), 100).unwrap();

        let result = manager.unbind_queue("nonexistent_exchange", "test_queue");
        assert!(result.is_err());
    }
}
