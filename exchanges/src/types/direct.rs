use log::info;
use std::collections::hash_map::Entry;
use std::collections::HashSet;

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

impl PushToQueueStrategy for DirectExchange {}

impl Exchange for DirectExchange {
    fn bind(
        &mut self,
        queue_name: &QueueName,
        metadata: Option<&Metadata>,
    ) -> Result<(), ExchangeError> {
        let metadata = match metadata {
            Some(data) => data,
            None => return Err(ExchangeError::BindFail),
        };

        let route_key = match &metadata.routing_key {
            Some(key) => key.value.clone(),
            None => return Err(ExchangeError::BindFail),
        };

        match self.routings_map.entry(route_key) {
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
        message: Message,
        queues: Arc<RwLock<QueueContainer>>,
        metadata: Option<&Metadata>,
    ) -> Result<u32, ExchangeError> {
        let metadata = match metadata {
            Some(data) => data,
            None => return Err(ExchangeError::BindFail),
        };

        let route_key = match &metadata.routing_key {
            Some(key) => key.value.clone(),
            None => return Err(ExchangeError::BindFail),
        };

        let bound_queues = match self.routings_map.get(&route_key) {
            Some(q) => q,
            None => {
                return Err(ExchangeError::NoMatchingQueue {
                    route_key: String::from(route_key),
                })
            }
        };

        let queues_read = queues.read().unwrap();
        let mut pushed_counter: u32 = 0;

        for name in bound_queues {
            if let Some(queue) = queues_read.get(name) {
                if self.push_to_queue(&self.exchange_name, message.clone(), queue, name, &queues_read)?
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
    use std::sync::Once;

    static ONCE: Once = Once::new();

    use protos::RoutingKey;
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
        // let map = HashMap::from([
        //     ("route_key".to_string(), "route_1".to_string())
        // ]);
        let metadata = Metadata {
            routing_key: Some(RoutingKey {
                value: "route_1".to_string(),
            }),
        };
        assert_eq!(ex.bind(&"q1".to_string(), Some(&metadata)), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), Some(&metadata)), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), Some(&metadata)), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicate_queue_different_routing_bind_test() {
        let mut ex = DirectExchange::default();
        // let map_first = HashMap::from([("route_key".to_string(), "test_1".to_string())]);
        // let map_second = HashMap::from([("route_key".to_string(), "test_2".to_string())]);
        let metadata_first = Metadata {
            routing_key: Some(RoutingKey {
                value: "test_1".to_string(),
            }),
        };
        let metadata_second = Metadata {
            routing_key: Some(RoutingKey {
                value: "test_2".to_string(),
            }),
        };
        assert!(ex.bind(&"q1".to_string(), Some(&metadata_first)).is_ok());
        assert!(ex.bind(&"q1".to_string(), Some(&metadata_second)).is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn duplicate_queue_same_routing_bind_test() {
        let mut ex = DirectExchange::default();
        // let map_first = HashMap::from([("route_key".to_string(), "test".to_string())]);
        // let map_second = HashMap::from([("route_key".to_string(), "test".to_string())]);
        let metadata_first = Metadata {
            routing_key: Some(RoutingKey {
                value: "test".to_string(),
            }),
        };
        let metadata_second = Metadata {
            routing_key: Some(RoutingKey {
                value: "test".to_string(),
            }),
        };
        assert!(ex.bind(&"q1".to_string(), Some(&metadata_first)).is_ok());
        assert!(ex.bind(&"q1".to_string(), Some(&metadata_second)).is_err());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn different_queue_bind_test() {
        let mut ex = DirectExchange::default();
        // let map_first = HashMap::from([("route_key".to_string(), "test".to_string())]);
        let metadata_first = Metadata {
            routing_key: Some(RoutingKey {
                value: "test".to_string(),
            }),
        };
        assert!(ex.bind(&"q1".to_string(), Some(&metadata_first)).is_ok());
        assert!(ex.bind(&"q2".to_string(), Some(&metadata_first)).is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 2);
    }

    #[test]
    fn direct_exchange_test() {
        ONCE.call_once(|| {
            env_logger::init();
        });
        let queues = setup_test_queues();
        let mut ex = DirectExchange::default();
        // let map_first = HashMap::from([("route_key".to_string(), "test_1".to_string())]);
        // let map_second = HashMap::from([("route_key".to_string(), "test_2".to_string())]);
        
        let metadata_first = Metadata {
            routing_key: Some(RoutingKey {
                value: "test_1".to_string(),
            }),
        };
        let metadata_second = Metadata {
            routing_key: Some(RoutingKey {
                value: "test_2".to_string(),
            }),
        };

        assert_eq!(ex.bind(&"q1".to_string(), Some(&metadata_first)), Ok(()));
        assert_eq!(ex.bind(&"q1".to_string(), Some(&metadata_second)), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), Some(&metadata_first)), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), Some(&metadata_first)), Ok(()));

        let message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
        };

        assert_eq!(ex.handle_message(message.clone(), queues.clone(), Some(&metadata_first)), Ok(3u32));
        assert_eq!(ex.handle_message(message, queues.clone(), Some(&metadata_first)), Ok(3u32));

        let message = Message {
            uuid: Uuid::new_v4().to_string(),
            payload: "#abadcaffe".to_string(),
        };

        assert_eq!(ex.handle_message(message, queues.clone(), Some(&metadata_second)), Ok(1u32));
    }
}
