use super::RoutingStrategy;
use dashmap::DashSet;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::sync::Arc;

pub struct FanoutStrategy;

impl RoutingStrategy for FanoutStrategy {
    fn route(&self, msg: Message, bindings: &DashSet<Arc<Queue>>) -> Result<(), RuusterError> {
        if bindings.is_empty() {
            return Err(RuusterError::InvalidMetadata(
                "Fanout failed: no queues bound".into(),
            ));
        }

        // Broadcast to all bound queues.
        // NOTE: This clones the message. If the payload is huge, this is expensive
        // unless we use Arc<Vec<u8>> in the Message struct.
        for queue in bindings.iter() {
            queue.enqueue(msg.clone())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashSet;
    use ruuster_core::Queue;
    use ruuster_internals::Message;
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn test_fanout_broadcast_logic() {
        let strategy = FanoutStrategy;
        let bindings = DashSet::new();

        let q1 = Arc::new(Queue::new("q1".into(), 10));
        let q2 = Arc::new(Queue::new("q2".into(), 10));

        bindings.insert(Arc::clone(&q1));
        bindings.insert(Arc::clone(&q2));

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: Some("anything".into()), // Should be ignored
            payload: Arc::new(vec![]),
            metadata: None,
        };

        strategy.route(msg, &bindings).unwrap();

        assert_eq!(q1.len(), 1);
        assert_eq!(q2.len(), 1);
    }
}
