use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

// Define the structure for the message queue
struct MessageQueue {
    queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl MessageQueue {
    // Create a new message queue
    fn new() -> Self {
        MessageQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    // Push a message onto the queue
    fn push(&self, message: Vec<u8>) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
    }

    // Receive a message from the queue
    fn receive(&self) -> Option<Vec<u8>> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }
}

fn main() {
    // Create a new queue
    let queue = MessageQueue::new();

    let mut threads = Vec::new();
    // Producer thread
    let producer = {
        for x in 1..1000 {
            let queue = queue.queue.clone();
            threads.push(thread::spawn(move || {
                let message = b"Hello, world!".to_vec();
                let mut queue = queue.lock().unwrap();
                queue.push_back(message);
                println!("Message pushed to queue");
            }));
        }
    };

    // Consumer thread
    let consumer = {
        let queue = queue.queue.clone();
        
        thread::spawn(move || {
            // thread::sleep(Duration::from_secs(1)); // Simulate wait for message
            let mut queue = queue.lock().unwrap();
            let mut counter = 0;
            while let Some(message) = queue.pop_front() {
                println!("#{} Message received: {:?}", counter, std::str::from_utf8(&message));
                counter+= 1;
            }
        })
    };

    consumer.join().unwrap();
    for p in threads {
        p.join().unwrap();
    }
}
