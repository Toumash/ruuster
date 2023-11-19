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
}