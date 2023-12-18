use exchanges::{ExchangeContainer, ExchangeKind, ExchangeName, ExchangeType};
use protos::Message;

use tonic::Status;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

pub type Uuid = String;
pub type QueueName = String;
pub type Queue = VecDeque<Message>;
pub type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

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

    fn add_queue(&self, queue_name: &QueueName) -> Result<(), Status> {
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

        queues_write.insert(queue_name.to_owned(), Mutex::new(VecDeque::new()));

        Ok(())
    }

    fn add_exchange(
        &self,
        exchange_name: &ExchangeName,
        exchange_kind: &ExchangeKind,
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
                &format!("failed to acquire exchanges lock: {}", e),
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

    fn get_queues_list(&self) -> Vec<QueueName> {
        todo!()
    }

    fn get_exchanges_list(&self) -> Vec<ExchangeName> {
        todo!()
    }

    fn get_bindings_list(&self, exchange_name: &ExchangeName) -> Vec<QueueName> {
        todo!()
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

    fn consume_message() -> Message {
        todo!()
    }

    fn log_status(message: &String, code: tonic::Code) -> Status {
        return Status::new(code, message);
    }

    /*
    let req = request.into_inner();

        let (queue_name, message_option) = {
            let queue_name = &req.queue_name;
            let queues_read = self.queues.read().unwrap();

            let requested_queue = match queues_read.get(queue_name) {
                Some(queue) => queue,
                None => {
                    let msg = "requested queue doesn't exist";
                    log::error!("{}", msg);
                    return Err(Status::not_found(msg));
                }
            };

            let mut queue = requested_queue.lock().unwrap();

            (queue_name, queue.pop_front())
        };

        if message_option.is_none() {
            let msg = format!("queue {} is empty", queue_name);
            log::warn!("{}", msg);
            return Err(Status::not_found(msg));
        }

        let message = message_option.unwrap();
        let uuid = &message.uuid;

        if req.auto_ack {
            self.ack_message(tonic::Request::new(AckRequest {
                uuid: uuid.to_string(),
            }))
            .await?;
        }

        Ok(Response::new(message)) */
}
