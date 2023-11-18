use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

type Message = String;
type Callback = fn(Message) -> ();
type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

struct FanoutExchange
{
    bound_queues: Vec<QueueName>,
}

impl FanoutExchange
{
    fn new() -> Self {
        FanoutExchange { bound_queues: Vec::new() }
    }

    fn bind_queue(&mut self, queue_name: String) {
        self.bound_queues.push(queue_name);
    }
}

struct Channel {
    queues : Arc<RwLock<QueueContainer>>,
    exchange: Arc<RwLock<FanoutExchange>>, // this should be generic ofc
}

impl Channel {
    fn new() -> Self {
        Channel { 
            queues: Arc::new(RwLock::new(HashMap::new())),
            exchange: Arc::new(RwLock::new(FanoutExchange::new())),
        }
    }

    fn queue_declare(&mut self, name: QueueName) {
        let mut queues_lck = self.queues.write().unwrap();
        queues_lck.insert(name.clone(), Mutex::new(VecDeque::new()));
        println!("Queue '{}' declared!", name);
    }

    fn basic_consume(&self, queue_name: QueueName, on_message_callback: Callback) -> JoinHandle<()> {
        let queues = self.queues.clone();
        let handle =thread::spawn(move || {
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
        handle
    }

    fn bind_queue_to_exchange(&self, queue_name: QueueName) {
        self.exchange.write().unwrap().bind_queue(queue_name);
    }

    // WE DONT NEED YOU ANYMORE :<
    // fn basic_publish(&mut self, queue_name: QueueName, body: Message) {
    //     let queues_read = self.queues.read().unwrap();
    //     if let Some(queue) = queues_read.get(&queue_name) {
    //         let mut queue_write = queue.lock().unwrap();
    //         queue_write.push_back(body.clone());
    //         println!("Message published to queue '{}', body: '{}'.", queue_name, body);
    //     } else {
    //         println!("Error: Queue '{}' not found.", queue_name);
    //     }
    // }

    fn publish_to_exchange(&self, body: Message)
    {
        let exchange = self.exchange.read().unwrap();
        for queue_name in &exchange.bound_queues {
            if let Some(queue) = self.queues.read().unwrap().get(queue_name) {
                queue.lock().unwrap().push_back(body.clone());
            }
        }
    }
}

fn main() {

    let mut channel = Channel::new();
    let mut handles = vec![];
    let name1 = "queue1";
    let name2 = "queue2";

    channel.queue_declare(name1.into());
    channel.queue_declare(name2.into());

    channel.bind_queue_to_exchange(name1.into());
    channel.bind_queue_to_exchange(name2.into());

    handles.push(channel.basic_consume(name1.into(), |message| {
        println!("queue1 | message: {}", message);
    }));

    handles.push(channel.basic_consume(name2.into(), |message| {
        println!("queue2 | message: {}", message);
    }));

    for i in 0..100 {
        channel.publish_to_exchange(format!("Log: {}", i));
        thread::sleep(Duration::from_millis(100));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}