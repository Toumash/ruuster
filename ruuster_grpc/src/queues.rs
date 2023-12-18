use exchanges::{ExchangeContainer, ExchangeKind, ExchangeName, ExchangeType};
use protos::Message;

use tonic::Status;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

pub type Uuid = String;
pub type QueueName = String;
pub type Queue = VecDeque<Message>;
pub type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
}

impl RuusterQueues {
    pub fn new() -> Self {
        RuusterQueues {
            queues: Arc::new(RwLock::new(QueueContainer::new())),
            exchanges: Arc::new(RwLock::new(ExchangeContainer::new())),
        }
    }

    pub fn add_queue(&self, queue_name: &QueueName) -> Result<(), Status> {
        let mut queues_write = self.queues.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire queue exclusive-lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        if queues_write.get(queue_name).is_some() {
            return Err(RuusterQueues::log_status(
                &format!("queue {} already exists", queue_name),
                tonic::Code::AlreadyExists,
            ));
        }

        queues_write.insert(queue_name.to_owned(), Arc::new(Mutex::new(VecDeque::new())));

        Ok(())
    }

    pub fn get_queue(&self, queue_name: &QueueName) -> Result<Arc<Mutex<Queue>>, Status> {
        let queues_read = self.queues.read().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acuire queues read-only lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        match queues_read.get(queue_name) {
            Some(queue) => Ok(queue.clone()),
            None => Err(RuusterQueues::log_status(
                &format!("queue {} not found", queue_name),
                tonic::Code::Unavailable,
            )),
        }
    }

    pub fn add_exchange(
        &self,
        exchange_name: &ExchangeName,
        exchange_kind: ExchangeKind,
    ) -> Result<(), Status> {
        let mut exchange_write = self.exchanges.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acuire exchange exclusive-lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        if exchange_write.get(exchange_name).is_some() {
            return Err(RuusterQueues::log_status(
                &format!("exchange {} already exists", exchange_name),
                tonic::Code::AlreadyExists,
            ));
        }

        exchange_write.insert(exchange_name.to_owned(), exchange_kind.create());

        Ok(())
    }

    fn get_exchange(
        &self,
        exchange_name: &ExchangeName,
    ) -> Result<Arc<RwLock<ExchangeType>>, Status> {
        let exchanges_read = self.exchanges.read().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire exchanges read-only lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        match exchanges_read.get(exchange_name) {
            Some(exchange) => Ok(exchange.clone()),
            None => Err(RuusterQueues::log_status(
                &format!("exchange {} not found", exchange_name),
                tonic::Code::Unavailable,
            )),
        }
    }

    pub fn get_queues_list(&self) -> Result<Vec<QueueName>, Status> {
        let queues_read = self.queues.read().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire queues read-only lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;
        Ok(queues_read.iter().map(|queue| queue.0.clone()).collect())
    }

    pub fn get_exchanges_list(&self) -> Result<Vec<ExchangeName>, Status> {
        let exchanges_read = self.exchanges.read().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire exchanges read-only lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;
        Ok(exchanges_read
            .iter()
            .map(|exchange| exchange.0.clone())
            .collect())
    }

    pub fn get_bindings_list(
        &self,
        exchange_name: &ExchangeName,
    ) -> Result<Vec<QueueName>, Status> {
        todo!()
        // let exchange = self.get_exchange(exchange_name)?;
        // let exchange_read = exchange.read().map_err(|e| {
        //     RuusterQueues::log_status(
        //         &format!(
        //             "failed to acquire exchange {} read-only lock: {}",
        //             exchange_name, e
        //         ),
        //         tonic::Code::Unavailable,
        //     )
        // })?;

        // Ok(exchange_read.get_bound_queue_names())
    }

    fn forward_message(
        &self,
        message: Message,
        exchange_name: &ExchangeName,
    ) -> Result<u32, Status> {
        let exchange = self.get_exchange(exchange_name)?;

        let result = {
            let exchange_read = exchange.read().map_err(|e| {
                RuusterQueues::log_status(
                    &format!("failed to acquire exchanges lock: {}", e),
                    tonic::Code::Unavailable,
                )
            })?;

            exchange_read
                .handle_message(&Some(message), self.queues.clone())
                .map_err(|e| {
                    RuusterQueues::log_status(
                        &format!("failed to handle message: {}", e),
                        tonic::Code::Internal,
                    )
                })
        };

        result
    }

    fn consume_message(&self, queue_name: &QueueName) -> Result<Message, Status> {
        let queue = self.get_queue(queue_name)?;

        let message = {
            queue
                .lock()
                .map_err(|e| {
                    RuusterQueues::log_status(
                        &format!("failed to acuire queue lock: {}", e),
                        tonic::Code::Unavailable,
                    )
                })?
                .pop_back()
        };

        match message {
            Some(msg) => Ok(msg),
            None => Err(Status::not_found("failed to return message")),
        }
    }

    fn log_status(message: &String, code: tonic::Code) -> Status {
        return Status::new(code, message);
    }
}
