use protos::Message;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

const DEFAULT_PREFETCH_COUNT: usize = 10;

pub type Uuid = String;
pub type QueueName = String;
pub type QueueData = VecDeque<Message>;

pub struct Queue {
    data: QueueData,
    current_prefetch_count: usize,
    prefetch_count: usize,
}

impl Queue {
    pub fn new(prefetch_count: usize) -> Self {
        Queue {
            data: QueueData::new(),
            current_prefetch_count: 0,
            prefetch_count,
        }
    }

    pub fn get_data(&self) -> &QueueData {
        &self.data
    }

    pub fn get_mut_data(&mut self) -> &mut QueueData {
        &mut self.data
    }

    pub fn increment_prefetched(&mut self) {
        self.current_prefetch_count += 1;
    }

    pub fn decrement_prefetched(&mut self, n: usize) {
        self.current_prefetch_count -= n;
    }

    pub fn is_prefetch_full(&self) -> bool {
        self.current_prefetch_count >= self.prefetch_count
    }
}

impl Default for Queue {
    fn default() -> Self {
        Queue::new(DEFAULT_PREFETCH_COUNT)
    }
}

pub type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;
