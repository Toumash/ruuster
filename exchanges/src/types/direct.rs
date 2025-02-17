use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, instrument};

use crate::*;

type RoutingKey = String;

#[derive(Default)]
pub struct DirectExchange {
    bound_queues: HashSet<QueueName>,
    routing_map: HashMap<QueueName, HashSet<RoutingKey>>, // allow multiple bindings exchange and queue
    exchange_name: ExchangeName,
    bind_count: u32,
}

impl DirectExchange {
    #[allow(dead_code)] // this is currently only used in tests
    fn new(exchange_name: ExchangeName) -> Self {
        DirectExchange {
            bound_queues: HashSet::new(),
            exchange_name,
            routing_map: HashMap::new(),
            bind_count: 0u32,
        }
    }

    fn get_routing_key(&self, metadata: Option<&protos::BindMetadata>) -> Option<RoutingKey> {
        if let Some(md) = metadata {
            return md.routing_key.as_ref().map(|key| key.value.clone());
        } else {
            return None;
        };
    }
}

impl PushToQueueStrategy for DirectExchange {}

impl Exchange for DirectExchange {
    #[instrument(skip_all, fields(exchange_name=%self.exchange_name, queue_name=%queue_name))]
    fn bind(
        &mut self,
        queue_name: &QueueName,
        metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), ExchangeError> {
        let routing_key = if let Some(rk) = self.get_routing_key(metadata) {
            rk
        } else {
            error!("routing_key is required for DirectExchange binding");
            return Err(ExchangeError::BindFail);
        };

        match self.routing_map.entry(queue_name.into()) {
            Entry::Occupied(mut entry) => {
                debug!("adding bind to an existing routing_map entry");
                if !entry.get_mut().insert(routing_key.clone()) {
                    error!("this binding already exists");
                    return Err(ExchangeError::BindFail);
                }
            }
            Entry::Vacant(_) => {
                debug!("adding new entry to routing_map");
                self.routing_map
                    .insert(queue_name.into(), HashSet::from([routing_key.clone()]));
            }
        };

        self.bound_queues.insert(queue_name.into());
        self.bind_count += 1;

        Ok(())
    }

    fn get_bound_queue_names(&self) -> HashSet<QueueName> {
        self.bound_queues.clone()
    }

    fn handle_message(
        &self,
        message: Message,
        queues: Arc<RwLock<QueueContainer>>,
    ) -> Result<u32, ExchangeError> {
        let metadata = message
            .metadata
            .as_ref()
            .ok_or(ExchangeError::MessageWithoutMetadata)?;

        let routing_map = &self.routing_map;

        let routing_key = metadata
            .routing_key
            .as_ref()
            .ok_or(ExchangeError::MissingRoutingKey)?;

        let queues_read = queues.read().unwrap();
        let mut pushed_counter: u32 = 0;

        for (queue_name, keys) in routing_map {
            if let Some(queue) = queues_read.get(queue_name) {
                for key in keys {
                    if key != routing_key {
                        continue;
                    }
                    if self.push_to_queue(
                        &self.exchange_name,
                        message.clone(),
                        queue,
                        queue_name,
                        &queues_read,
                    )? == PushResult::Ok
                    {
                        pushed_counter += 1;
                    }
                }
            }
        }

        Ok(pushed_counter)
    }

    fn get_bind_count(&self) -> u32 {
        self.bind_count
    }

    fn unbind(
        &mut self,
        queue_name: &QueueName,
        metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), ExchangeError> {
        let routing_key = if let Some(rk) = self.get_routing_key(metadata) {
            rk
        } else {
            error!("routing_key is required for unbinding operation");
            return Err(ExchangeError::MissingRoutingKey);
        };

        let routing_set = if let Some(rs) = self.routing_map.get_mut(queue_name) {
            rs
        } else {
            error!("queue has no routing_set assigned");
            return Err(ExchangeError::RoutingSetNotFound);
        };

        if !routing_set.remove(&routing_key) {
            return Err(ExchangeError::RoutingKeyNotFound);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    static ONCE: Once = Once::new();

    use protos::{BindMetadata, RoutingKey};
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
        let bind_metadata = BindMetadata {
            routing_key: Some(RoutingKey {
                value: "route_1".to_string(),
            }),
        };
        assert_eq!(ex.bind(&"q1".to_string(), Some(&bind_metadata)), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), Some(&bind_metadata)), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), Some(&bind_metadata)), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn bind_count_test() {
        let mut ex: DirectExchange = DirectExchange::default();
        let bind_metadata1 = BindMetadata {
            routing_key: Some(RoutingKey {
                value: "route_1".to_string(),
            }),
        };
        let bind_metadata2 = BindMetadata {
            routing_key: Some(RoutingKey {
                value: "route_2".to_string(),
            }),
        };
        assert_eq!(ex.bind(&"q1".to_string(), Some(&bind_metadata1)), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), Some(&bind_metadata1)), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), Some(&bind_metadata1)), Ok(()));
        assert_eq!(ex.bind(&"q1".to_string(), Some(&bind_metadata2)), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), Some(&bind_metadata2)), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), Some(&bind_metadata2)), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
        assert_eq!(ex.get_bind_count(), 6);
    }

    #[test]
    fn duplicate_queue_different_routing_bind_test() {
        let mut ex = DirectExchange::default();
        assert!(ex
            .bind(
                &"q1".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_1".to_string()
                    })
                })
            )
            .is_ok());
        assert!(ex
            .bind(
                &"q1".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_2".to_string()
                    })
                })
            )
            .is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn duplicate_queue_same_routing_bind_test() {
        let mut ex = DirectExchange::default();
        let meta = protos::BindMetadata {
            routing_key: Some(protos::RoutingKey {
                value: "test_1".to_string(),
            }),
        };
        assert!(ex.bind(&"q1".to_string(), Some(&meta)).is_ok());
        assert!(ex.bind(&"q1".to_string(), Some(&meta)).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn different_queue_bind_test() {
        let mut ex = DirectExchange::default();
        assert!(ex
            .bind(
                &"q1".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_1".to_string()
                    })
                })
            )
            .is_ok());
        assert!(ex
            .bind(
                &"q2".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_1".to_string()
                    })
                })
            )
            .is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 2);
    }

    #[test]
    fn direct_exchange_test() {
        ONCE.call_once(|| {
            env_logger::init();
        });
        let queues = setup_test_queues();
        let mut ex = DirectExchange::default();

        assert_eq!(
            ex.bind(
                &"q1".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_1".to_string()
                    })
                })
            ),
            Ok(())
        );
        assert_eq!(
            ex.bind(
                &"q1".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_2".to_string()
                    })
                })
            ),
            Ok(())
        );
        assert_eq!(
            ex.bind(
                &"q2".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_1".to_string()
                    })
                })
            ),
            Ok(())
        );
        assert_eq!(
            ex.bind(
                &"q3".to_string(),
                Some(&protos::BindMetadata {
                    routing_key: Some(protos::RoutingKey {
                        value: "test_1".to_string()
                    })
                })
            ),
            Ok(())
        );

        let message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: Some(Metadata {
                routing_key: Some("test_1".to_string()),
                created_at: None,
                dead_letter: None,
            }),
        };

        assert_eq!(ex.handle_message(message.clone(), queues.clone()), Ok(3u32));
        assert_eq!(ex.handle_message(message, queues.clone()), Ok(3u32));

        let message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: Some(Metadata {
                routing_key: Some("test_2".to_string()),
                created_at: None,
                dead_letter: None,
            }),
        };

        assert_eq!(ex.handle_message(message, queues.clone()), Ok(1u32));
    }

    #[test]
    fn direct_exchange_will_send_message_to_dead_letter_queue_when_full() {
        let queues = setup_test_queues();
        let mut queues_write = queues.write().unwrap();
        queues_write.insert(
            "_deadletter".to_string(),
            Arc::new(Mutex::new(Queue::new())),
        );
        drop(queues_write);
        let mut ex = DirectExchange::new("fanout_test".into());
        let routing_metadata = Metadata {
            routing_key: Some("route_key".to_string()),
            created_at: None,
            dead_letter: None,
        };
        let _ = ex.bind(
            &"q1".to_string(),
            Some(&protos::BindMetadata {
                routing_key: Some(RoutingKey {
                    value: "route_key".to_string(),
                }),
            }),
        );

        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    Message {
                        uuid: Uuid::new_v4().to_string(),
                        payload: "#abadcaffe".to_string(),
                        metadata: Some(routing_metadata.clone()),
                    },
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: Some(routing_metadata),
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
    fn direct_exchange_will_drop_message_when_deadletter_queue_does_not_exist() {
        // arrange
        let queues = setup_test_queues();
        // we don't queues.insert here so there's no dead letter queue
        let queues_read = queues.read().unwrap();
        let dead_letter_queue = queues_read.get(DEADLETTER_QUEUE_NAME);
        assert!(dead_letter_queue.is_none());

        let mut ex = DirectExchange::new("fanout_test".into());
        let _ = ex.bind(
            &"q1".to_string(),
            Some(&BindMetadata {
                routing_key: Some(RoutingKey {
                    value: "route_key".to_string(),
                }),
            }),
        );

        // add the messages up to the limit
        for _ in 1..=1000 {
            let _ = ex
                .handle_message(
                    Message {
                        uuid: Uuid::new_v4().to_string(),
                        payload: "#abadcaffe".to_string(),
                        metadata: Some(Metadata {
                            created_at: None,
                            dead_letter: None,
                            routing_key: Some("route_key".to_string()),
                        }),
                    },
                    queues.clone(),
                )
                .unwrap();
        }

        let one_too_many_message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
            metadata: Some(Metadata {
                created_at: None,
                dead_letter: None,
                routing_key: Some("route_key".to_string()),
            }),
        };

        // act
        let message_handled_by_queues_count = ex
            .handle_message(one_too_many_message, queues.clone())
            .unwrap();

        // assert
        assert_eq!(message_handled_by_queues_count, 0);
    }
}
