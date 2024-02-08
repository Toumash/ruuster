use std::collections::HashSet;
use log::info;

use crate::*;

#[derive(Default)]
pub struct DirectExchange {
    bound_queues: HashSet<QueueName>,
    routings_map: HashMap<String, HashSet<QueueName>>,
}


impl Exchange for DirectExchange {
    fn bind(&mut self, queue_name: &QueueName, metadata: &QueueMetadata) -> Result<(), ExchangeError> {

        match metadata.get("route_key") {
            Some(k) => self.routings_map
                .entry(k.to_string())
                .and_modify(|v| {
                    v.insert(queue_name.to_string());
                })
                .or_insert(HashSet::from([queue_name.to_string()])),
            None => return Err(ExchangeError::BindFail { reason: "Empty routing key".to_string() })
        }; 

        
        if !self.bound_queues.insert(queue_name.clone()) {
            info!("Updating queue named: ");
        }

        Ok(())
    }

    fn get_bound_queue_names(&self) -> &HashSet<QueueName> {
        &self.bound_queues
    }

    fn handle_message(
        &self,
        message: &Option<Message>,
        queues: Arc<RwLock<QueueContainer>>
    ) -> Result<u32, ExchangeError> {
        if message.is_none() {
            return Err(ExchangeError::EmptyPayloadFail {
                reason: "sent message has no content".to_string(),
            });
        }

        let msg = match message {
            Some(m) => m,
            None => return Err(ExchangeError::EmptyPayloadFail { reason: "empty message".to_string() })
        };

        let route_key = match msg.header.get("route_key") {
            Some(k) => k,
            None => return Err(ExchangeError::NoRouteKey)
        };

        let bound_queues = match self.routings_map.get(route_key) {
            Some(q) => q,
            None => return Err(ExchangeError::NoMatchingQueue { route_key: String::from(route_key) } )
        };

        let queues_read = queues.read().unwrap();
        let mut pushed_counter: u32 = 0;

        for name in bound_queues {
            if let Some(queue) = queues_read.get(name) {
                queue.lock().unwrap().push_back(message.clone().unwrap());
                pushed_counter+=1;
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
        let map = HashMap::from([
            ("route_key".to_string(), "test".to_string())
        ]);
        assert_eq!(ex.bind(&"q1".to_string(), &map), Ok(()));
        assert_eq!(ex.bind(&"q2".to_string(), &map), Ok(()));
        assert_eq!(ex.bind(&"q3".to_string(), &map), Ok(()));
        assert_eq!(ex.get_bound_queue_names().len(), 3);
    }

    #[test]
    fn duplicates_bind_test() {
        let mut ex = DirectExchange::default();
        let map_first = HashMap::from([
            ("route_key".to_string(), "test".to_string())
        ]);
        let map_second = HashMap::from([
            ("route_key".to_string(), "test".to_string())
        ]);
        assert_eq!(ex.bind(&"q1".to_string(), &map_first), Ok(()));
        assert!(ex.bind(&"q1".to_string(), &map_first).is_ok());
        assert!(ex.bind(&"q1".to_string(), &map_second).is_ok());
        assert_eq!(ex.get_bound_queue_names().len(), 1);
    }

    #[test]
    fn direct_exchange_test() {

        ONCE.call_once(|| {
            env_logger::init();
        });
        let queues = setup_test_queues();
        let mut ex = DirectExchange::default();
        let map_first = HashMap::from([
            ("route_key".to_string(), "test".to_string())
        ]);
        let map_second = HashMap::from([
            ("route_key".to_string(), "test2".to_string())
        ]);
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
}
