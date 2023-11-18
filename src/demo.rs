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
    queues: HashMap<String, Arc<Queue>>,
    exchanges: HashMap<String, Arc<Mutex<Exchange>>>,
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
        self.queues.insert(queue, q.into());
    }

    fn declare_exchange(&mut self, exchange: Name) {
        let name_copy = exchange.clone();
        let e = Exchange::new(name_copy);
        let v = Mutex::from(e).into();
        self.exchanges.insert(exchange, v);
    }

    fn bind_queue(&mut self, exchange_name: Name, queue_name: Name) -> Option<SyncSender<Message>> {
        let queue = self.queues.get(&queue_name);
        let exchange = self.exchanges.get(&exchange_name);

        match (queue, exchange) {
            (Some(q), Some(e)) => {
                let sender = q.channel.0.clone();
                e.lock().unwrap().bound_channel = Some(sender);
                return Some(q.channel.0.clone());
            }
            (_, _) => {
                print!("no queue/exchange with this name 3");
                return None;
            }
        };
    }

    fn basic_publish(&self, exchange_name: &Name, message: Message) {
        match self.exchanges.get(exchange_name) {
            Some(e) => e.lock().unwrap().basic_publish(message),
            None => println!("No exchange with this name"),
        }
    }

    fn basic_consume(&self, queue_name: &Name, on_message_callback: CallbackType) {
        match self.queues.get(queue_name) {
            Some(q) => q.receive(on_message_callback),
            None => todo!(),
        }
    }

    // fn run(&self) {
    //     for key in self.queues.keys() {
    //         let queue = &self.queues.get(key).unwrap();
    //         queue.run();
    //     }
    // }
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

    fn basic_publish(&self, message: Message) {
        self.bound_channel.as_ref().unwrap().send(message);
    }
}

struct Queue {
    queue: Arc<Mutex<VecDeque<Message>>>,
    channel: (SyncSender<Message>, Arc<Receiver<Message>>),
}

impl Queue {
    fn new() -> Self {
        let c = sync_channel(1000);
        Queue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            channel: (c.0, c.1.into()),
        }
    }

    // fn run(&self) {
    //     let mut moved_channel = self.channel.1;
    //     loop {
    //         if let message = moved_channel.recv().unwrap() {
    //             self.queue.lock().unwrap().push_back(message);
    //             // on_message_callback(message);
    //         }
    //         thread::sleep(Duration::from_micros(100))
    //     }
    // }

    fn push(&self, message: Message) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
    }

    fn receive(&self, on_message_callback: CallbackType) {
        let mutex = &self.queue;
        loop {
            let msg = self.channel.1.recv().unwrap();
            self.queue.lock().unwrap().push_back(msg);

            let mut queue_write = mutex.lock().unwrap();
            if let Some(message) = queue_write.pop_front() {
                drop(queue_write); // Explicitly drop lock before callback call to prevent dead-lock in case of time consuming callback
                on_message_callback(message.clone());
            }
            thread::sleep(Duration::from_micros(100))
        }
    }
}

fn on_receive(message: Message) {
    println!("Message: {:?}", String::from_utf8(message));
}

fn main() {
    let mut broker = MessageBroker::new();
    broker.declare_queue(String::from("queue"));
    broker.declare_exchange(String::from("exchange"));
    let e = broker
        .bind_queue(String::from("exchange"), String::from("queue"))
        .unwrap();

    // Producer thread
    for x in 1..1000 {
        let message = b"Hello, world!".to_vec();
        e.send(message);
        println!("Message pushed to queue");
    }

    broker.basic_consume(&String::from("queue"), |message| {
        println!("Message: {:?}", String::from_utf8(message));
    });
}
