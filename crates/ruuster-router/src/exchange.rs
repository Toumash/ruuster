use crate::strategies::RoutingStrategy;
use dashmap::DashSet;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::sync::Arc;

pub struct Exchange {
    pub name: String,
    bindings: DashSet<Arc<Queue>>,
    strategy: Box<dyn RoutingStrategy>,
}

impl Exchange {
    pub fn new(name: String, strategy: Box<dyn RoutingStrategy>) -> Self {
        Self {
            name,
            bindings: DashSet::new(),
            strategy,
        }
    }

    pub fn bind(&self, queue: Arc<Queue>) {
        self.bindings.insert(queue);
    }

    pub fn unbind(&self, queue_name: &str) {
        self.bindings.retain(|q| q.name != queue_name);
    }

    pub fn route(&self, msg: Message) -> Result<(), RuusterError> {
        // Delegate the work to the strategy
        self.strategy.route(msg, &self.bindings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::RoutingStrategy;
    use ruuster_internals::RuusterError;

    // A simple Mock Strategy to test delegation without logic
    struct MockStrategy;
    impl RoutingStrategy for MockStrategy {
        fn route(
            &self,
            _msg: Message,
            _bindings: &DashSet<Arc<Queue>>,
        ) -> Result<(), RuusterError> {
            Ok(()) // Just say yes
        }
    }

    #[test]
    fn test_exchange_binding_management() {
        let ex = Exchange::new("test_ex".into(), Box::new(MockStrategy));
        let q = Arc::new(Queue::new("q".into(), 10));

        ex.bind(Arc::clone(&q));
        assert_eq!(ex.bindings.len(), 1);

        ex.unbind("q");
        assert_eq!(ex.bindings.len(), 0);
    }
}
