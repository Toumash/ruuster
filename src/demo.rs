use std::collections::HashMap;
use std::collections::VecDeque;
use std::string;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

type CallbackType = fn(Message) -> ();
type Message = Vec<u8>;
type Name = String;

struct MessageBroker {
    queues: HashMap<String, Queue>,
    exchanges: HashMap<String, Exchange>,
}

impl MessageBroker {
    fn new() -> Self {
        MessageBroker {
            queues: HashMap::new(),
            exchanges: HashMap::new(),
        }
    }

    fn declare_queue(&mut self, queue: Name) {
        let q = Queue::new();
        q.run();
        self.queues.insert(queue, q);
    }

    fn declare_exchange(&mut self, exchange: Name) {
        let name_copy = exchange.clone();
        self.exchanges.insert(exchange, Exchange::new(name_copy));
    }

    fn bind_queue(&mut self, exchange_name: Name, queue_name: Name) {
        let queue = self.queues.get(&queue_name);
        let exchange = self.exchanges.get(&exchange_name);

        match (queue, exchange) {
            (Some(q), Some(e)) => {
                let sender = q.channel.0.clone();
                e.bound_channel = Some(sender);
            }
            (_, _) => {
                print!("no queue/exchange with this name 3");
            }
        }
    }

    fn basic_publish(&self, exchange_name: &Name, message: Message) {
        match self.exchanges.get(exchange_name) {
            Some(e) => e.basic_publish(message),
            None => println!("No exchange with this name"),
        }
    }

    fn basic_consume(&self, queue_name: &Name, on_message_callback: CallbackType) {
        match self.queues.get(queue_name) {
            Some(q) => q.receive(on_message_callback),
            None => todo!(),
        }
    }

    fn run(&self) {
        for key in self.queues.keys() {
            let queue = &self.queues.get(key).unwrap();
            thread::spawn(|| {
                queue.run();
            });
        }
    }
}

struct Exchange {
    name: String,
    bound_channel: Option<std::sync::mpsc::SyncSender<Message>>,
}
impl Exchange {
    fn new(name: Name) -> Self {
        Exchange {
            name: name,
            bound_channel: None,
        }
    }

    fn bind(&mut self, sender: SyncSender<Message>) {
        self.bound_channel = Some(sender);
    }

    fn basic_publish(&mut self, message: Message) {
        match self.bound_channel {
            Some(c) => {
                c.send(message);
            }
            None => todo!(),
        }
    }
}

struct Queue {
    queue: Arc<Mutex<VecDeque<Message>>>,
    channel: (SyncSender<Message>, Receiver<Message>),
}

impl Queue {
    fn new() -> Self {
        Queue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            channel: sync_channel(1000),
        }
    }

    fn run(&self) {
        let mut moved_channel = self.channel.1;
        loop {
            if let message = moved_channel.recv().unwrap() {
                self.queue.lock().unwrap().push_back(message);
                // on_message_callback(message);
            }
            thread::sleep(Duration::from_micros(100))
        }
    }

    fn push(&self, message: Message) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
    }

    fn receive(&self, on_message_callback: CallbackType) {
        thread::spawn(move || {
            loop {
                let mut queue_write = self.queue.lock().unwrap();
                if let Some(message) = queue_write.pop_front() {
                    drop(queue_write); // Explicitly drop lock before callback call to prevent dead-lock in case of time consuming callback
                    on_message_callback(message);
                }
                thread::sleep(Duration::from_micros(100))
            }
        });
    }
}

fn on_receive(message: Message) {
    print!("Message: {:?}", message);
}

fn main() {
    let mut broker = MessageBroker::new();
    broker.declare_queue(String::from("queue"));
    broker.declare_exchange(String::from("exchange"));
    broker.bind_queue(String::from("exchange"), String::from("queue"));

    broker.basic_consume(&String::from("exchange"), on_receive);

    let mut threads = Vec::new();
    // Producer thread
    for x in 1..1000 {
        threads.push(thread::spawn(move || {
            let message = b"Hello, world!".to_vec();
            broker.basic_publish(&String::from("main"), message);
            println!("Message pushed to queue");
        }));
    }
    broker.run();

    for p in threads {
        p.join().unwrap();
    }
}
