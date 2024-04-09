use log::info;
use serde_json::json;
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::*;

#[derive(Default)]
pub struct DirectExchange {
    bound_queues: HashSet<QueueName>,
    routings_map: HashMap<String, HashSet<QueueName>>,
    exchange_name: String,
}

impl DirectExchange {
    #[allow(dead_code)] // this is currently only used in tests
    fn new(exchange_name: String) -> Self {
        DirectExchange {
            bound_queues: HashSet::new(),
            exchange_name,
            routings_map: HashMap::new(),
        }
    }
}
pub const QUEUE_MAX_LENGTH: usize = 1_000;
pub const DEADLETTER_QUEUE_NAME: &str = "_deadletter";

impl Exchange for DirectExchange {
    fn bind(
        &mut self,
        queue_name: &QueueName,
        metadata: &QueueMetadata,
    ) -> Result<(), ExchangeError> {
        let route_key = match metadata.get("route_key") {
            Some(k) => k,
            None => return Err(ExchangeError::BindFail),
        };

        match self.routings_map.entry(route_key.to_string()) {
            Entry::Occupied(o) => {
                let value = o.into_mut();
                match value.get(queue_name) {
                    Some(_) => return Err(ExchangeError::BindFail),
                    None => value.insert(queue_name.to_string()),
                };
            }
            Entry::Vacant(v) => {
                v.insert(HashSet::from([queue_name.to_string()]));
            }
        };

        if !self.bound_queues.insert(queue_name.clone()) {
            info!("Updating queue named: {}", queue_name);
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
        let msg = match message {
            Some(m) => m,
            None => return Err(ExchangeError::EmptyPayloadFail),
        };

        let route_key = match msg.header.get("route_key") {
            Some(k) => k,
            None => return Err(ExchangeError::NoRouteKey),
        };

        let bound_queues = match self.routings_map.get(route_key) {
            Some(q) => q,
            None => {
                return Err(ExchangeError::NoMatchingQueue {
                    route_key: String::from(route_key),
                })
            }
        };

        let queues_read = queues.read().unwrap();
        let mut pushed_counter: u32 = 0;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .map_err(|_| ExchangeError::GetSystemTimeFail {})?;

        for name in bound_queues {
            if let Some(queue) = queues_read.get(name) {
                let msg = message.clone().unwrap();
                let queue_lock = &mut queue.lock().unwrap();
                if queue_lock.len() >= QUEUE_MAX_LENGTH {
                    log::warn!("queue size reached for queue {}", name);
                    if let Some(dead_letter_queue) = queues_read.get(DEADLETTER_QUEUE_NAME) {
                        // FIXME: use the deadletter queue defined per exchange
                        log::debug!("moving the message {} to the dead letter queue", msg.uuid);

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

                        dead_letter_queue
                            .lock()
                            .map_err(|_| ExchangeError::DeadLetterQueueLockFail {})?
                            .push_back(Message {
                                uuid: msg.uuid,
                                header: HashMap::new(),
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
    use std::sync::Once;

    static ONCE: Once = Once::new();

    use uuid::Uuid;

    use super::*;

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
        let mut ex: DirectExchange = DirectExchange::default();
        let map = HashMap::from([("route_key".to_string(), "route_1".to_string())]);
        assert_eq!(ex.bind(&"q1".to_string(), &map), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), &map), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), &map), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicate_queue_different_routing_bind_test() {
        let mut ex = DirectExchange::default();
        let map_first = HashMap::from([("route_key".to_string(), "test_1".to_string())]);
        let map_second = HashMap::from([("route_key".to_string(), "test_2".to_string())]);
        assert!(ex.bind(&"q1".to_string(), &map_first).is_ok());
        assert!(ex.bind(&"q1".to_string(), &map_second).is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn duplicate_queue_same_routing_bind_test() {
        let mut ex = DirectExchange::default();
        let map_first = HashMap::from([("route_key".to_string(), "test".to_string())]);
        let map_second = HashMap::from([("route_key".to_string(), "test".to_string())]);
        assert!(ex.bind(&"q1".to_string(), &map_first).is_ok());
        assert!(ex.bind(&"q1".to_string(), &map_second).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn different_queue_bind_test() {
        let mut ex = DirectExchange::default();
        let map_first = HashMap::from([("route_key".to_string(), "test".to_string())]);
        assert!(ex.bind(&"q1".to_string(), &map_first).is_ok());
        assert!(ex.bind(&"q2".to_string(), &map_first).is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 2);
    }

    #[test]
    fn direct_exchange_test() {
        ONCE.call_once(|| {
            env_logger::init();
        });
        let queues = setup_test_queues();
        let mut ex = DirectExchange::default();
        let map_first = HashMap::from([("route_key".to_string(), "test_1".to_string())]);
        let map_second = HashMap::from([("route_key".to_string(), "test_2".to_string())]);
        assert_eq!(ex.bind(&"q1".to_string(), &map_first), Ok(()));
        assert_eq!(ex.bind(&"q1".to_string(), &map_second), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), &map_first), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), &map_first), Ok(()));

        let message = Some(Message {
            uuid: Uuid::new_v4().to_string(),
            header: map_first.clone(),
            payload: "#abadcaffe".to_string(),
        });

        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(3u32));
        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(3u32));

        let message = Some(Message {
            uuid: Uuid::new_v4().to_string(),
            header: map_second.clone(),
            payload: "#abadcaffe".to_string(),
        });

        assert_eq!(ex.handle_message(&message, queues.clone()), Ok(1u32));
    }

    #[test]
    fn direct_exchange_will_send_message_to_dead_letter_queue_when_full() {
        // arrange
        let queues = setup_test_queues();
        let mut queues_write = queues.write().unwrap();
        queues_write.insert(
            "_deadletter".to_string(),
            Arc::new(Mutex::new(Queue::new())),
        );
        drop(queues_write);
        let mut ex = DirectExchange::new("fanout_test".into());
        let routing_header = HashMap::from([("route_key".to_string(), "test_1".to_string())]);
        let _ = ex.bind(&"q1".to_string(), &routing_header);

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    &(Some(Message {
                        uuid: Uuid::new_v4().to_string(),
                        header: routing_header.clone(),
                        payload: "#abadcaffe".to_string(),
                    })),
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            header: routing_header.clone(),
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
    fn direct_exchange_will_drop_message_when_deadletter_queue_does_not_exist() {
        // arrange
        let queues = setup_test_queues();
        // we dont queues.insert here so theres no dead letter queue
        let queues_read = queues.read().unwrap();
        let dead_letter_queue = queues_read.get(DEADLETTER_QUEUE_NAME);
        assert!(dead_letter_queue.is_none());

        let mut ex = DirectExchange::new("fanout_test".into());
        let routing_header = HashMap::from([("route_key".to_string(), "test_1".to_string())]);
        let _ = ex.bind(&"q1".to_string(), &routing_header);

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    &(Some(Message {
                        uuid: Uuid::new_v4().to_string(),
                        header: routing_header.clone(),
                        payload: "#abadcaffe".to_string(),
                    })),
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            header: routing_header.clone(),
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
