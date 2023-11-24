use crate::*;

#[derive(Default)]
pub struct FanoutExchange
{
    bound_queues: Vec<QueueName>,
}

impl Exchange for FanoutExchange {
    fn bind(&mut self, queue_name: &QueueName) {
        self.bound_queues.push(queue_name.clone());
    }

    fn get_bound_queue_names(&self) -> &[QueueName] {
        &self.bound_queues
    }

    fn handle_message(&self, message: &Message, queues: Arc<RwLock<QueueContainer>>){
        let queues_names = self.get_bound_queue_names();
        let queues_read = queues.read().unwrap();

        for name in queues_names {
            if let Some(queue) = queues_read.get(name) {
                queue.lock().unwrap().push_back(message.clone());
            }
        }
    }
}