use std::collections::HashSet;

use crate::*;

#[derive(Default)]
pub struct FanoutExchange {
    bound_queues: HashSet<QueueName>,
    exchange_name: String,
}

impl FanoutExchange {
    #[allow(dead_code)] // this is currently only used in tests
    fn new(exchange_name: String) -> Self {
        FanoutExchange {
            bound_queues: HashSet::new(),
            exchange_name,
        }
    }
}
impl PushToQueueStrategy for FanoutExchange {}

impl Exchange for FanoutExchange {
    fn bind(
        &mut self,
        queue_name: &QueueName,
        _metadata: &QueueMetadata,
    ) -> Result<(), ExchangeError> {
        if !self.bound_queues.insert(queue_name.clone()) {
            return Err(ExchangeError::BindFail);
        }
        Ok(())
    }

    fn get_bound_queue_names(&self) -> HashSet<QueueName> {
        self.bound_queues.clone()
    }

    fn handle_message(
        &self,
        message: &Option<Message>,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError> {
        if message.is_none() {
            return Err(ExchangeError::EmptyPayloadFail);
        }
        let queues_names = self.get_bound_queue_names();
        let queues_read = queues.read().unwrap();

        let mut pushed_counter: u32 = 0;

        for name in queues_names {
            if let Some(queue) = queues_read.get(&name) {
                if self.push_to_queue(&self.exchange_name, message, queue, &name, &queues_read)?
                    == PushResult::Ok
                {
                    pushed_counter += 1;
                }
            }
        }

        Ok(pushed_counter)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use lazy_static::lazy_static;
    lazy_static! {
        static ref MAP: HashMap<String, String> = HashMap::new();
    }

    fn setup_test_queues() -> Arc<RwLock<QueueContainer>> {
        let queues = Arc::new(RwLock::new(QueueContainer::new()));
        let mut queues_write = queues.write().unwrap();
        queues_write.insert("q1".to_string(), Arc::new(Mutex::new(Queue::new())));
        queues_write.insert("q2".to_string(), Arc::new(Mutex::new(Queue::new())));
        queues_write.insert("q3".to_string(), Arc::new(Mutex::new(Queue::new())));
        drop(queues_write);
        queues
    }

    #[test]
    fn bind_test() {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string(), &MAP), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), &MAP), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), &MAP), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicates_bind_test() {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string(), &MAP), Ok(()));
        assert!(ex.bind(&"q1".to_string(), &MAP).is_err());
        assert!(ex.bind(&"q1".to_string(), &MAP).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn fanout_exchange_test() {
        let queues = setup_test_queues();
        let mut ex = FanoutExchange::default();

        assert_eq!(ex.bind(&"q1".to_string(), &MAP), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), &MAP), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), &MAP), Ok(()));

        let message = Some(Message {
            uuid: Uuid::new_v4().to_string(),
            header: MAP.clone(),
            payload: "#abadcaffe".to_string(),
        });

        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(3u32));
        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(3u32));
        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(3u32));

        let queues_read = queues.read().unwrap();
        for (_, queue_mutex) in queues_read.iter() {
            let queue = queue_mutex.lock().unwrap();
            assert_eq!(queue.len(), 3, "Queue does not have exactly 3 messages");
        }
    }

    #[test]
    fn fanout_exchange_will_send_message_to_dead_letter_queue_when_full() {
        // arrange
        let queues = setup_test_queues();
        let mut queues_write = queues.write().unwrap();
        queues_write.insert(
            "_deadletter".to_string(),
            Arc::new(Mutex::new(Queue::new())),
        );
        drop(queues_write);
        let mut ex = FanoutExchange::new("fanout_test".into());
        let _ = ex.bind(&"q1".to_string(), &ExchangeMetadata::new());

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    &(Some(Message {
                        uuid: Uuid::new_v4().to_string(),
                        header: HashMap::new(),
                        payload: "#abadcaffe".to_string(),
                    })),
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            header: HashMap::new(),
            payload: "#abadcaffe".to_string(),
        };

        // act
        let _ = ex
            .handle_message(&(Some(one_too_many_message.clone())), queues.clone())
            .unwrap();

        // assert
        let queues_read = queues.read().unwrap();
        let dead_letter_queue = &queues_read["_deadletter"];
        assert_eq!(dead_letter_queue.lock().unwrap().len(), 1, "messages should be places into the dead letter queue after the capacity of a queue has been exhausted");
        assert_eq!(
            dead_letter_queue.lock().unwrap().pop_front().unwrap().uuid,
            one_too_many_message.uuid
        );
    }

    #[test]
    fn fanout_exchange_will_drop_message_when_deadletter_queue_does_not_exist() {
        // arrange
        let queues = setup_test_queues();
        // we dont queues.insert here so theres no dead letter queue
        let queues_read = queues.read().unwrap();
        let dead_letter_queue = queues_read.get(DEADLETTER_QUEUE_NAME);
        assert!(dead_letter_queue.is_none());

        let mut ex = FanoutExchange::new("fanout_test".into());
        let _ = ex.bind(&"q1".to_string(), &ExchangeMetadata::new());

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    &(Some(Message {
                        uuid: Uuid::new_v4().to_string(),
                        header: HashMap::new(),
                        payload: "#abadcaffe".to_string(),
                    })),
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            header: HashMap::new(),
            payload: "#abadcaffe".to_string(),
        };

        // act
        let message_handled_by_queues_count = ex
            .handle_message(&(Some(one_too_many_message.clone())), queues.clone())
            .unwrap();

        // assert
        assert_eq!(message_handled_by_queues_count, 0);
    }
}
