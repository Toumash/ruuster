// In this example program spawns two threads
// * first - consumer that will infinitely wait for messages incoming into queue
// * second - producer that will publish 100 messages into queue
// note that we can add another consumer thread on the same queue but
// we dont have any meaningful way to predict which consumer will "eat" message
// we can also add another queue to hashmap in runtime (thanks Rust Gods for RwLock)

use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

type Message = String;
type Callback = fn(Message) -> ();
type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

struct Channel {
    queues : Arc<RwLock<QueueContainer>>,
}

impl Channel {
    fn new() -> Self {
        Channel { queues: Arc::new(RwLock::new(HashMap::new())), }
    }

    fn queue_declare(&mut self, name: QueueName) {
        let mut queues_lck = self.queues.write().unwrap();
        queues_lck.insert(name.clone(), Mutex::new(VecDeque::new()));
        println!("Queue '{}' declared!", name);
    }

    fn basic_consume(&self, queue_name: QueueName, on_message_callback: Callback) {
        let queues = self.queues.clone();
        thread::spawn(move || {
            loop {
                let queues_read = queues.read().unwrap();
                if let Some(queue) = queues_read.get(&queue_name){
                    let mut queue_write = queue.lock().unwrap();
                    if let Some(message) = queue_write.pop_front()
                    {
                        drop(queue_write); // Explicitly drop lock before callback call to prevent dead-lock in case of time consuming callback
                        on_message_callback(message);
                    }
                }
                else {
                    println!("Queue '{}'", queue_name);
                }
            }
        });
    }

    fn basic_publish(&mut self, queue_name: QueueName, body: Message) {
        let queues_read = self.queues.read().unwrap();
        if let Some(queue) = queues_read.get(&queue_name) {
            let mut queue_write = queue.lock().unwrap();
            queue_write.push_back(body.clone());
            println!("Message published to queue '{}', body: '{}'.", queue_name, body);
        } else {
            println!("Error: Queue '{}' not found.", queue_name);
        }
    }
}


fn create_publish_thread(mut channel: Channel) {
    let publish_thread = thread::spawn(move || {
        let mut i = 0;
        loop {
            match i {
                0..=100 => {
                    let message = format!("Message {}", i);
                    channel.basic_publish("hello".into(), message);
                    i+=1;
                },
                _ => break,
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    publish_thread.join().expect("Error joining publish thread");
}

fn main() {
    let mut channel = Channel::new();
    channel.queue_declare("hello".into());

    channel.basic_consume("hello".into(), |message| {
        println!("Received message: {}", message);
    });

    create_publish_thread(channel);
}
