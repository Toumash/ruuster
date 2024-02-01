use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::*;

pub const QUEUE_MAX_LENGTH: usize = 1_000;
pub const DEADLETTER_QUEUE_NAME: &str = "_deadletter";

#[derive(Default)]
pub struct FanoutExchange {
    bound_queues: HashSet<QueueName>,
    exchange_name: String,
}

impl FanoutExchange {
    fn new(exchange_name: String) -> Self {
        FanoutExchange {
            bound_queues: HashSet::new(),
            exchange_name: exchange_name,
        }
    }
}

impl Exchange for FanoutExchange {
    fn bind(&mut self, queue_name: &QueueName) -> Result<(), ExchangeError> {
        if !self.bound_queues.insert(queue_name.clone()) {
            return Err(ExchangeError::BindFail {
                reason: "name of queue must be unique".to_string(),
            });
        }
        Ok(())
    }

    fn get_bound_queue_names(&self) -> &HashSet<QueueName> {
        &self.bound_queues
    }

    fn handle_message(
        &self,
        message: &Option<Message>,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError> {
        if message.is_none() {
            return Err(ExchangeError::EmptyPayloadFail {
                reason: "sent message has no content".to_string(),
            });
        }
        let queues_names = self.get_bound_queue_names();
        let queues_read = queues.read().unwrap();

        let mut pushed_counter: u32 = 0;

        for name in queues_names {
            let msg = message.clone().unwrap();

            if let Some(queue) = queues_read.get(name) {
                let queue_lock = &mut queue.lock().unwrap();

                if queue_lock.len() >= QUEUE_MAX_LENGTH {
                    log::warn!("queue size reached for queue {}", name);

                    if let Some(dead_letter_queue) = queues_read.get(DEADLETTER_QUEUE_NAME) {
                        // FIXME: use the deadletter queue defined per exchange
                        log::debug!("moving the message {} to the dead letter queue", msg.uuid);
                        let now = SystemTime::now();
                        let timestamp = match now.duration_since(UNIX_EPOCH) {
                            Ok(duration) => duration.as_millis() as i64,
                            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
                        };

                        // FIXME: convert to ruuster headers
                        let val = json!({
                            "count": 1,
                            "exchange": self.exchange_name,
                            "original_message": msg.payload,
                            "reason": "max_len",
                            "time": timestamp,
                            "queue": name.to_string(),
                        })
                        .to_string();

                        dead_letter_queue.lock().unwrap().push_back(Message {
                            uuid: msg.uuid,
                            payload: val,
                        });
                    } else {
                        log::debug!("message {} dropped", msg.uuid);
                    }
                } else {
                    queue_lock.push_back(message.clone().unwrap());
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
    fn bind_test() {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string()), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicates_bind_test() {
        let mut ex = FanoutExchange::default();
        assert_eq!(ex.bind(&"q1".to_string()), Ok(()));
        assert!(ex.bind(&"q1".to_string()).is_err());
        assert!(ex.bind(&"q1".to_string()).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn fanout_exchange_test() {
        let queues = setup_test_queues();
        let mut ex = FanoutExchange::default();

        assert_eq!(ex.bind(&"q1".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string()), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string()), Ok(()));

        let message = Some(Message {
            uuid: Uuid::new_v4().to_string(),
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
        queues_write.insert("_deadletter".to_string(), Mutex::new(Queue::new()));
        drop(queues_write);
        let mut ex = FanoutExchange::new("fanout_test".into());
        let _ = ex.bind(&"q1".to_string());

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    &(Some(Message {
                        uuid: Uuid::new_v4().to_string(),
                        payload: "#abadcaffe".to_string(),
                    })),
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
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
}
