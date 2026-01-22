use super::RoutingStrategy;
use dashmap::DashSet;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::sync::Arc;

pub struct DirectStrategy;

impl RoutingStrategy for DirectStrategy {
    fn route(&self, msg: Message, bindings: &DashSet<Arc<Queue>>) -> Result<(), RuusterError> {
        // Think of better error naming
        let key = msg
            .routing_key
            .as_deref()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| {
                RuusterError::InvalidMetadata(
                    "Direct routing requires a non-empty routing key".to_string(),
                )
            })?;

        let target = bindings.iter().find(|q| q.name == key);

        if let Some(queue) = target {
            queue.enqueue(msg)?;
            Ok(())
        } else {
            // returns an error (or sends to a Dead Letter Exchange)
            Err(RuusterError::InvalidMetadata(format!(
                "No queue bound to key: {}",
                key
            )))
        }
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
    fn test_direct_routing_logic() {
        let strategy = DirectStrategy;
        let bindings = DashSet::new();

        let target_q = Arc::new(Queue::new("target".into(), 10));
        let other_q = Arc::new(Queue::new("other".into(), 10));

        bindings.insert(Arc::clone(&target_q));
        bindings.insert(Arc::clone(&other_q));

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: Some("target".into()),
            payload: Arc::new(vec![]),
            metadata: None,
        };

        strategy.route(msg, &bindings).unwrap();

        assert_eq!(target_q.len(), 1);
        assert_eq!(other_q.len(), 0);
    }
}
