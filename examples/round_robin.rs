// Here we have another example which is extention of first one
// Now program spawns WORKERS_COUNT amount of consumers and they "eat" messages one by one
// in cyclic fashion, so now we have an ability to determine 
// which thread will consume next message
// Additionally I reduced busy-waits and sleeps on threads using conditional variables

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

type Message = String;
type WorkerId = usize;
type Callback = fn(Message, &WorkerId) -> ();
type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

const WORKERS_COUNT: usize = 3;

struct Channel {
    queues: Arc<RwLock<QueueContainer>>,
    next_worker: Arc<Mutex<WorkerId>>,
    notification: Arc<Condvar>, // conditional variable to prevent consumer threads from busy wating
}

impl Channel {
    fn new() -> Self {
        Channel {
            queues: Arc::new(RwLock::new(HashMap::new())),
            next_worker: Arc::new(Mutex::new(0)),
            notification: Arc::new(Condvar::new()),
        }
    }

    fn queue_declare(&mut self, name: QueueName) {
        let mut queues_lck = self.queues.write().unwrap();
        queues_lck.insert(name.clone(), Mutex::new(VecDeque::new()));
        println!("Queue '{}' declared!", name);
    }

    fn basic_consume(
        &self,
        queue_name: QueueName,
        worker_id: WorkerId,
        on_message_callback: Callback,
    ) -> JoinHandle<()> {
        let queues: Arc<RwLock<HashMap<String, Mutex<VecDeque<String>>>>> = self.queues.clone();
        let next_worker = self.next_worker.clone();
        let notification = self.notification.clone();
        let handle = thread::spawn(move || {
            loop {
                let mut nw = next_worker.lock().unwrap();

                while worker_id != *nw {
                    // notice that 'wait' implicitly unlocks mutex
                    nw = notification.wait(nw).unwrap();
                }

                *nw = (*nw + 1) % WORKERS_COUNT;
                drop(nw);
                notification.notify_all();

                let queues_read = queues.read().unwrap();
                if let Some(queue) = queues_read.get(&queue_name) {
                    let mut queue_write = queue.lock().unwrap();
                    if let Some(message) = queue_write.pop_front() {
                        drop(queue_write); // explicitly drop lock before callback call to prevent dead-lock in case of time consuming callback
                        on_message_callback(message, &worker_id);
                    }
                } else {
                    println!("Queue '{}'", queue_name);
                }
            }
        });

        handle
    }

    fn basic_publish(&mut self, queue_name: QueueName, body: Message) {
        let queues_read = self.queues.read().unwrap();
        if let Some(queue) = queues_read.get(&queue_name) {
            let mut queue_write = queue.lock().unwrap();
            queue_write.push_back(body.clone());
            drop(queue_write);
        } else {
            println!("Error: Queue '{}' not found.", queue_name);
        }
    }
}

fn process_message(message: Message, worker_id: &usize) {
    println!(
        "Start of processing: '{}' by worker with id {}",
        message, worker_id
    );
    thread::sleep(Duration::from_secs(1));
    println!("End of processing: '{}'", message);
}

fn main() {
    let queue_name = "queue1";
    let mut channel = Channel::new();
    let mut handles = vec![];
    channel.queue_declare(queue_name.into());

    for i in 0..WORKERS_COUNT {
        let handle = channel.basic_consume(queue_name.into(), i, process_message);
        handles.push(handle)
    }

    for i in 0..20 {
        let message = format!("Message: {}", i);
        channel.basic_publish(queue_name.into(), message);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
