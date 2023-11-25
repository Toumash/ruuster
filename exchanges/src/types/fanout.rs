use std::collections::HashSet;

use crate::*;

#[derive(Default)]
pub struct FanoutExchange
{
    bound_queues: HashSet<QueueName>,
}

impl Exchange for FanoutExchange {
    fn bind(&mut self, queue_name: &QueueName) -> Result<(), ExchangeError> {
        if !self.bound_queues.insert(queue_name.clone()) {
            return Err(ExchangeError::BindFail { reason: "name of queue must be unique".to_string() });
        }
        Ok(())
    }

    fn get_bound_queue_names(&self) -> &HashSet<QueueName> {
        &self.bound_queues
    }

    fn handle_message(&self, message: &Message, queues: Arc<RwLock<QueueContainer>>) -> Result<(), ExchangeError> {
        let queues_names = self.get_bound_queue_names();
        let queues_read = queues.read().unwrap();

        for name in queues_names {
            if let Some(queue) = queues_read.get(name) {
                queue.lock().unwrap().push_back(message.clone());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protos::ruuster::{MessageHeader, MessageBody};

    fn setup_test_queues() -> Arc<RwLock<QueueContainer>> {
        let queues = Arc::new(RwLock::new(QueueContainer::new()));
        let mut queues_write = queues.write().unwrap();
        queues_write.insert("q1".to_string(), Mutex::new(Queue::new()));
        queues_write.insert("q2".to_string(), Mutex::new(Queue::new()));
        queues_write.insert("q3".to_string(), Mutex::new(Queue::new()));
        drop(queues_write);
        queues
    }

    #[test]
    fn bind_test()
    {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string()), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicates_bind_test()
    {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string()), Ok(()));
        assert!(ex.bind(&"q1".to_string()).is_err());
        assert!(ex.bind(&"q1".to_string()).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn fanout_exchange_test()
    {
        let queues = setup_test_queues();
        let mut ex = FanoutExchange::default();

        assert_eq!(ex.bind(&"q1".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string()), Ok(()));

        let message = Message {
            header: Some(MessageHeader {
                exchange_name: "e1".to_string(),
            }),
            body: Some(MessageBody {
                payload: format!("abadcaffe"),
            }),
        };

        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(()));
        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(()));
        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(()));

        let queues_read = queues.read().unwrap();
        for (_, queue_mutex) in queues_read.iter() {
            let queue = queue_mutex.lock().unwrap();
            assert_eq!(queue.len(), 3, "Queue does not have exactly 3 messages");
        }
    }
}