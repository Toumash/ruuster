use exchanges::{ExchangeContainer, ExchangeKind, ExchangeName, ExchangeType};
use protos::Message;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::acks::{AckContainer, ApplyAck};

pub type Uuid = String;
pub type QueueName = String;
pub type Queue = VecDeque<Message>;
pub type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
    acks: Arc<RwLock<AckContainer>>,
}

const DEFAULT_ACK_DURATION: Duration = Duration::from_secs(60);

impl RuusterQueues {
    pub fn new() -> Self {
        RuusterQueues {
            queues: Arc::new(RwLock::new(QueueContainer::new())),
            exchanges: Arc::new(RwLock::new(ExchangeContainer::new())),
            acks: Arc::new(RwLock::new(AckContainer::new())),
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
        let mut exchanges_write = self.exchanges.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acuire exchange exclusive-lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        if exchanges_write.get(exchange_name).is_some() {
            return Err(RuusterQueues::log_status(
                &format!("exchange {} already exists", exchange_name),
                tonic::Code::AlreadyExists,
            ));
        }

        exchanges_write.insert(exchange_name.to_owned(), exchange_kind.create());

        Ok(())
    }

    pub fn get_exchange(
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
        let exchange = self.get_exchange(exchange_name)?;
        let exchange_read = exchange.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!(
                    "failed to acquire exchange {} exclusive lock: {}",
                    exchange_name, e
                ),
                tonic::Code::Unavailable,
            )
        })?;

        Ok(exchange_read.get_bound_queue_names())
    }

    pub fn bind_queue_to_exchange(
        &self,
        queue_name: &QueueName,
        exchange_name: &ExchangeName,
    ) -> Result<(), Status> {
        let exchange = self.get_exchange(exchange_name)?;
        let mut exchange_write = exchange.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!(
                    "failed to acquire exchange {} exclusive lock: {}",
                    exchange_name, e
                ),
                tonic::Code::Unavailable,
            )
        })?;
        exchange_write.bind(queue_name).map_err(|e| {
            RuusterQueues::log_status(
                &format!(
                    "failed to bind queue {} to echange {}: {}",
                    queue_name, exchange_name, e
                ),
                tonic::Code::Internal,
            )
        })
    }

    pub fn forward_message(
        &self,
        message: &Option<Message>,
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
                .handle_message(message, self.queues.clone())
                .map_err(|e| {
                    RuusterQueues::log_status(
                        &format!("failed to handle message: {}", e),
                        tonic::Code::Internal,
                    )
                })
        };

        result
    }

    pub fn apply_message_ack(&self, uuid: Uuid) -> Result<(), Status> {
        let mut acks = self.acks.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire acks lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        acks.apply_ack(&uuid)?;
        acks.clear_unused_record(&uuid)?;
        Ok(())
    }

    pub fn track_message_delivery(&self, message: Message, duration: Duration) -> Result<(), Status> {
        let mut acks = self.acks.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire acks lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        acks.add_record(message, duration);

        Ok(())
    }

    pub fn consume_message(&self, queue_name: &QueueName) -> Result<Message, Status> {
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
                .pop_front()
        };

        match message {
            Some(msg) => {
                // NOTICE(msaff): I'm not sure how to avoid clone of message object here and I'm open to suggestions
                self.track_message_delivery(msg.clone(), DEFAULT_ACK_DURATION)?;
                return Ok(msg);
            }
            None => Err(Status::not_found("failed to return message")),
        }
    }

    pub async fn start_consuming_task(
        &self,
        queue_name: &QueueName,
    ) -> ReceiverStream<Result<Message, Status>> {
        let (tx, rx) = mpsc::channel(4);
        let queues = self.queues.clone();
        let queue_name = queue_name.clone();

        tokio::spawn(async move {
            loop {
                let message: Option<Message> = {
                    let queues_read = queues.read().unwrap();

                    let requested_queue = match queues_read.get(&queue_name) {
                        Some(queue) => queue,
                        None => {
                            let msg = "requested queue doesn't exist";
                            log::error!("{}", msg);
                            return Status::not_found(msg);
                        }
                    };

                    let mut queue = requested_queue.lock().unwrap();
                    queue.pop_front()
                };

                if let Some(message) = message {
                    if let Err(e) = tx.send(Ok(message)).await {
                        let msg = format!("error while sending message to channel: {}", e);
                        log::error!("{}", msg);
                        return Status::internal(msg);
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
        return ReceiverStream::new(rx);
    }

    fn log_status(message: &String, code: tonic::Code) -> Status {
        return Status::new(code, message);
    }
}
