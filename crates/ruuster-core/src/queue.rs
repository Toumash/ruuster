use ruuster_internals::{Message, RuusterError};
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub struct Queue {
    pub name: String,
    buffer: Mutex<VecDeque<Message>>,
    max_capacity: usize,
    pub notify: Arc<Notify>,
}

impl Queue {
    pub fn new(name: String, max_capacity: usize) -> Self {
        Self {
            name,
            buffer: Mutex::new(VecDeque::new()),
            max_capacity,
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn enqueue(&self, mut msg: Message) -> Result<(), RuusterError> {
        if let Some(ref mut meta) = msg.metadata {
            meta.arrival_time = Some(std::time::Instant::now());
        }

        // Implement errors - check if it's better to pass get_ref to InternalError (consider changing internal error to lock error)
        // Also: this function is sync since it takes nanoseconds to complete
        // If the sync lock time would be significant, consider using async Mutex from tokio
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|_| RuusterError::InternalError("Mutex poisoned".to_string()))?;

        if buffer.len() >= self.max_capacity {
            return Err(RuusterError::QueueFull(self.name.clone()));
        }

        buffer.push_back(msg);

        self.notify.notify_waiters();
        Ok(())
    }

    pub fn dequeue(&self) -> Result<Option<Message>, RuusterError> {
        // If the lock is poisoned, we return None, it's silent failure for now. Consider better error handling.
        // update: now the return type to Result<Option<Message>, RuusterError> - check if that's good approach
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|_| RuusterError::InternalError("Mutex poisoned".to_string()))?;
        Ok(buffer.pop_front())
    }

    pub fn len(&self) -> usize {
        self.buffer.lock().map(|b| b.len()).unwrap_or(0) // Silent failure on poisoned lock
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn purge(&self) {
        if let Ok(mut buffer) = self.buffer.lock() {
            buffer.clear();
        }
    }
}

impl PartialEq for Queue {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Queue {}

impl Hash for Queue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruuster_internals::{Message, MessageMetadata};
    use std::time::SystemTime;
    use uuid::Uuid;

    // Helper to create a message as if it just came from the conversion layer
    // (Meaning it has a client_time but NO arrival_time yet)
    fn create_incoming_msg() -> Message {
        Message {
            uuid: Uuid::new_v4(),
            routing_key: Some("test.key".into()),
            payload: b"payload".to_vec().into(),
            metadata: Some(MessageMetadata {
                client_time: Some(SystemTime::now()),
                arrival_time: None, // The Queue should fill this
            }),
        }
    }

    #[test]
    fn test_enqueue_stamps_arrival_time() {
        let queue = Queue::new("timestamp_q".into(), 10);
        let msg = create_incoming_msg();

        assert!(msg.metadata.as_ref().unwrap().arrival_time.is_none());

        queue.enqueue(msg).expect("Enqueue failed");

        let processed_msg = queue
            .dequeue()
            .expect("Dequeue failed")
            .expect("Message missing");
        let metadata = processed_msg.metadata.unwrap();

        // ASSERT: The queue successfully added the arrival timestamp
        assert!(metadata.arrival_time.is_some());
        // ASSERT: The client time was preserved
        assert!(metadata.client_time.is_some());
    }

    #[test]
    fn test_queue_fifo_integrity() {
        let queue = Queue::new("fifo_q".into(), 5);
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut msg1 = create_incoming_msg();
        msg1.uuid = id1;
        let mut msg2 = create_incoming_msg();
        msg2.uuid = id2;

        queue.enqueue(msg1).unwrap();
        queue.enqueue(msg2).unwrap();

        let item_1 = queue
            .dequeue()
            .expect("dequeue should succeed")
            .expect("message should be present");
        assert_eq!(item_1.uuid, id1);

        let item_2 = queue
            .dequeue()
            .expect("dequeue should succeed")
            .expect("message should be present");
        assert_eq!(item_2.uuid, id2);
    }

    #[test]
    fn test_backpressure_limit() {
        let queue = Queue::new("limit_q".into(), 1);

        // First one works
        queue.enqueue(create_incoming_msg()).unwrap();

        // Second one fails with QueueFull error
        let result = queue.enqueue(create_incoming_msg());
        match result {
            Err(ruuster_internals::RuusterError::QueueFull(name)) => assert_eq!(name, "limit_q"),
            _ => panic!("Expected QueueFull error, got {:?}", result),
        }
    }

    #[test]
    fn test_concurrent_enqueueing() {
        use std::sync::Arc;
        use std::thread;

        let queue = Arc::new(Queue::new("concurrent_q".into(), 100));
        let mut handles = vec![];

        for _ in 0..10 {
            let q = Arc::clone(&queue);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    q.enqueue(create_incoming_msg()).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(queue.len(), 100);
    }
}
