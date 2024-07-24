use std::collections::HashSet;

use tracing::{info, error, instrument, Span};

use crate::*;

#[derive(Default)]
pub struct FanoutExchange {
    bound_queues: HashSet<QueueName>,
    exchange_name: String,
    bind_count: u32
}

impl FanoutExchange {
    #[allow(dead_code)] // this is currently only used in tests
    fn new(exchange_name: String) -> Self {
        FanoutExchange {
            bound_queues: HashSet::new(),
            exchange_name,
            bind_count: 0u32
        }
    }
}

impl PushToQueueStrategy for FanoutExchange {}

impl Exchange for FanoutExchange {
    fn bind(
        &mut self,
        queue_name: &QueueName,
        _metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), ExchangeError> {
        if !self.bound_queues.insert(queue_name.clone()) {
            error!("bind with this name already exists");
            return Err(ExchangeError::BindFail);
        }

        self.bind_count += 1;

        Ok(())
    }

    fn get_bound_queue_names(&self) -> HashSet<QueueName> {
        self.bound_queues.clone()
    }

    #[instrument(skip_all, fields(uuid = %message.uuid))]
    fn handle_message(
        &self,
        message: Message,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError> {
        let _span = Span::current().entered();

        info!("handling message started");

        let queues_names = self.get_bound_queue_names();
        let queues_read = queues.read().unwrap();

        let mut pushed_counter: u32 = 0;

        for name in queues_names {
            if let Some(queue) = queues_read.get(&name) {
                if self.push_to_queue(
                    &self.exchange_name,
                    message.clone(),
                    queue,
                    &name,
                    &queues_read,
                )? == PushResult::Ok
                {
                    pushed_counter += 1;
                }
                info!(queue_name=%name, "message pushed");
            }
        }

        info!("handling message finished");

        Ok(pushed_counter)
    }
    
    fn get_bind_count(&self) -> u32 {
        self.bind_count
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    /**
     * Creates 3 queues with names: "q1", "q2", "q3"
     */
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
        assert_eq!(ex.bind(&"q1".to_string(), None), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), None), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), None), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicates_bind_test() {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string(), None), Ok(()));
        assert!(ex.bind(&"q1".to_string(), None).is_err());
        assert!(ex.bind(&"q1".to_string(), None).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn fanout_exchange_test() {
        let queues = setup_test_queues();
        let mut ex = FanoutExchange::default();

        assert_eq!(ex.bind(&"q1".to_string(), None), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), None), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), None), Ok(()));

        let message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: None,
        };

        assert_eq!(ex.handle_message(message.clone(), queues.clone()), Ok(3u32));
        assert_eq!(ex.handle_message(message.clone(), queues.clone()), Ok(3u32));
        assert_eq!(ex.handle_message(message, queues.clone()), Ok(3u32));

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
        let _ = ex.bind(&"q1".to_string(), None);

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    Message {
                        uuid: Uuid::new_v4().to_string(),
                        payload: "#abadcaffe".to_string(),
                        metadata: None,
                    },
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: None,
        };

        // act
        let _ = ex
            .handle_message(one_too_many_message.clone(), queues.clone())
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
        let _ = ex.bind(&"q1".to_string(), None);

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    Message {
                        uuid: Uuid::new_v4().to_string(),
                        payload: "#abadcaffe".to_string(),
                        metadata: None,
                    },
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: None,
        };

        // act
        let message_handled_by_queues_count = ex
            .handle_message(one_too_many_message, queues.clone())
            .unwrap();

        // assert
        assert_eq!(message_handled_by_queues_count, 0);
    }

    #[test]
    fn bind_count_test() {
        let _queues = setup_test_queues();
        let mut ex = FanoutExchange::default();

        assert_eq!(ex.bind(&"q1".to_string(), None), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), None), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), None), Ok(()));

        assert_eq!(ex.get_bind_count(), 3u32);
    }
}
