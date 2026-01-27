use dashmap::DashMap;
use ruuster_core::Queue;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Default)]
pub struct ConsumerManager {
    consumers: DashMap<Uuid, ConsumerState>,
}

struct ConsumerState {
    //consumer_id: Uuid, // Check if needed
    //queue_name: String, // Check if needed (we can get name from queue)
    queue: Arc<Queue>, // Check if needed
    prefetch_count: u16,
    unacked_messages: Vec<Uuid>, // Message IDs awaiting ack - TODO
    last_activity: std::time::Instant,
}

impl ConsumerManager {
    pub fn new() -> Self {
        Self {
            consumers: DashMap::new(),
        }
    }

    /// Register a new consumer
    pub fn register_consumer(&self, queue: Arc<Queue>, prefetch_count: u16) -> Uuid {
        let consumer_id = Uuid::new_v4();
        let state = ConsumerState {
            //consumer_id,
            //queue_name: queue.name.clone(),
            queue,
            prefetch_count,
            unacked_messages: Vec::new(),
            last_activity: std::time::Instant::now(),
        };
        self.consumers.insert(consumer_id, state);
        consumer_id
    }

    /// Check if consumer can receive more messages (prefetch limit)
    pub fn can_consume(&self, consumer_id: &Uuid) -> bool {
        self.consumers
            .get(consumer_id)
            .map(|c| c.unacked_messages.len() < c.prefetch_count as usize)
            .unwrap_or(false)
    }

    /// Track message delivery (before ack)
    pub fn track_delivery(&self, consumer_id: &Uuid, message_id: Uuid) {
        if let Some(mut consumer) = self.consumers.get_mut(consumer_id) {
            consumer.unacked_messages.push(message_id);
            consumer.last_activity = std::time::Instant::now();
        }
    }

    /// Remove tracked message (after ack)
    pub fn untrack_message(&self, consumer_id: &Uuid, message_id: &Uuid) {
        if let Some(mut consumer) = self.consumers.get_mut(consumer_id) {
            consumer.unacked_messages.retain(|id| id != message_id);
        }
    }

    /// Cleanup dead consumers (heartbeat timeout)
    pub fn cleanup_stale_consumers(&self, timeout: std::time::Duration) {
        let now = std::time::Instant::now();
        self.consumers
            .retain(|_, consumer| now.duration_since(consumer.last_activity) < timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruuster_core::Queue;
    use std::thread;
    use std::time::Duration;

    fn create_test_queue(name: &str) -> Arc<Queue> {
        Arc::new(Queue::new(name.to_string(), 100))
    }

    #[test]
    fn test_register_consumer() {
        let manager = ConsumerManager::new();
        let queue = create_test_queue("test_queue");

        let consumer_id = manager.register_consumer(queue, 10);

        // Consumer should be able to consume (no messages tracked yet)
        assert!(manager.can_consume(&consumer_id));
    }

    #[test]
    fn test_can_consume_respects_prefetch_limit() {
        let manager = ConsumerManager::new();
        let queue = create_test_queue("test_queue");
        let prefetch_count = 3;

        let consumer_id = manager.register_consumer(queue, prefetch_count);

        // Track messages up to prefetch limit
        for _ in 0..prefetch_count {
            let msg_id = Uuid::new_v4();
            manager.track_delivery(&consumer_id, msg_id);
        }

        // Should not be able to consume more
        assert!(!manager.can_consume(&consumer_id));
    }

    #[test]
    fn test_track_and_untrack_message() {
        let manager = ConsumerManager::new();
        let queue = create_test_queue("test_queue");

        let consumer_id = manager.register_consumer(queue, 5);
        let msg_id = Uuid::new_v4();

        // Track a message
        manager.track_delivery(&consumer_id, msg_id);

        // Untrack it
        manager.untrack_message(&consumer_id, &msg_id);

        // Should be able to consume again
        assert!(manager.can_consume(&consumer_id));
    }

    #[test]
    fn test_can_consume_with_nonexistent_consumer() {
        let manager = ConsumerManager::new();
        let fake_id = Uuid::new_v4();

        // Should return false for nonexistent consumer
        assert!(!manager.can_consume(&fake_id));
    }

    #[test]
    fn test_cleanup_stale_consumers() {
        let manager = ConsumerManager::new();
        let queue = create_test_queue("test_queue");

        let consumer_id = manager.register_consumer(queue, 5);

        // Wait a bit
        thread::sleep(Duration::from_millis(50));

        // Cleanup consumers older than 100ms (should not remove)
        manager.cleanup_stale_consumers(Duration::from_millis(100));
        assert!(manager.can_consume(&consumer_id));

        // Cleanup consumers older than 10ms (should remove)
        manager.cleanup_stale_consumers(Duration::from_millis(10));
        assert!(!manager.can_consume(&consumer_id));
    }

    #[test]
    fn test_track_delivery_updates_last_activity() {
        let manager = ConsumerManager::new();
        let queue = create_test_queue("test_queue");

        let consumer_id = manager.register_consumer(queue, 5);

        // Wait a bit
        thread::sleep(Duration::from_millis(50));

        // Track a message (should update last_activity)
        let msg_id = Uuid::new_v4();
        manager.track_delivery(&consumer_id, msg_id);

        // Should not be cleaned up with short timeout
        manager.cleanup_stale_consumers(Duration::from_millis(10));
        assert!(manager.can_consume(&consumer_id));
    }
}
