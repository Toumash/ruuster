use exchanges::{ExchangeContainer, ExchangeKind, ExchangeName, ExchangeType};
use protos::{Message, Metadata};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use uuid::Uuid;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::time::Duration;

use crate::acks::{AckContainer, ApplyAck};

pub type Payload = String;
pub type QueueName = String;
pub type Queue = VecDeque<Message>;
pub type QueueContainer = HashMap<QueueName, Arc<Mutex<Queue>>>;
pub type UuidSerialized = String;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
    acks: Arc<RwLock<AckContainer>>,
}

const DEFAULT_ACK_DURATION: Duration = Duration::from_secs(60);

impl Default for RuusterQueues {
    fn default() -> Self {
        Self {
            queues: Arc::new(RwLock::new(QueueContainer::new())),
            exchanges: Arc::new(RwLock::new(ExchangeContainer::new())),
            acks: Arc::new(RwLock::new(AckContainer::new())),
        }
    }
}

impl RuusterQueues {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_queue(&self, queue_name: &QueueName) -> Result<(), Status> {
        log::debug!("adding queue: {}", &queue_name);
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
        
        log::debug!("queue: {} added", &queue_name);
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
        log::debug!("adding exchange: {}", &exchange_name);
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

        log::debug!("exchange: {} added", &exchange_name);

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

    pub fn bind_queue_to_exchange(
        &self,
        queue_name: &QueueName,
        exchange_name: &ExchangeName,
        metadata: Option<&Metadata>
    ) -> Result<(), Status> {
        log::debug!("binding queue: {} to exchange: {}", queue_name, exchange_name);
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
        exchange_write.bind(queue_name, metadata).map_err(|e| {
            RuusterQueues::log_status(
                &format!(
                    "failed to bind queue {} to exchange {}: {}",
                    queue_name, exchange_name, e
                ),
                tonic::Code::Internal,
            )
        })?;
        log::debug!("binding queue: {} to exchange: {} completed", queue_name, exchange_name);
        Ok(())
    }

    pub fn forward_message(
        &self,
        payload: Payload,
        exchange_name: &ExchangeName,
        metadata: Option<Metadata>
    ) -> Result<u32, Status> {
        let uuid = Uuid::new_v4().to_string();
        log::debug!("forwarding message with uuid: {} to exchange: {}", uuid, exchange_name);
        let exchange = self.get_exchange(exchange_name)?;

        let result = {
            let exchange_read = exchange.read().map_err(|e| {
                RuusterQueues::log_status(
                    &format!("failed to acquire exchanges lock: {}", e),
                    tonic::Code::Unavailable,
                )
            })?;

            let message = Message { uuid: uuid.clone(), payload, metadata };

            exchange_read
                .handle_message(message, self.queues.clone())
                .map_err(|e| {
                    RuusterQueues::log_status(
                        &format!("failed to handle message: {}", e),
                        tonic::Code::Internal,
                    )
                })
        };
        log::debug!("message forwarding completed (uuid: {}, exchange: {})", uuid, exchange_name);
        result
    }

    fn get_acks(&self) -> Result<RwLockWriteGuard<'_, AckContainer>, Status> {
        let acks = self.acks.write().map_err(|e| {
            RuusterQueues::log_status(
                &format!("failed to acquire acks lock: {}", e),
                tonic::Code::Unavailable,
            )
        })?;

        Ok(acks)
    }

    pub fn apply_message_ack(&self, uuid: UuidSerialized) -> Result<(), Status> {
        log::debug!("single message ack for msg with uuid: {}", &uuid);
        let mut acks = self.get_acks()?;

        acks.apply_ack(&uuid)?;
        acks.clear_unused_record(&uuid)?;
        log::debug!("ack for message with uuid: {} completed", &uuid);
        Ok(())
    }

    pub fn apply_message_bulk_ack(&self, uuids: &[UuidSerialized]) -> Result<(), Status> {
        log::debug!("acking multiple messages");
        let mut acks = self.get_acks()?;
        acks.apply_bulk_ack(uuids)?;
        acks.clear_all_unused_records()?;
        log::debug!("acking multiple messages completed, acked uuids: {:#?}", uuids);
        Ok(())
    }

    fn track_message_delivery(
        acks: &mut AckContainer,
        message: Message,
        duration: Duration,
    ) -> Result<(), Status> {
        log::debug!("started tracking delivery for message: {}", &message.uuid);
        acks.add_record(message, duration);
        Ok(())
    }

    pub fn consume_message(
        &self,
        queue_name: &QueueName,
        auto_ack: bool,
    ) -> Result<Message, Status> {
        log::debug!("started consuming single message from queue: {}", queue_name);
        let queue = self.get_queue(queue_name)?;
        let mut acks = self.get_acks()?;

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
                if !auto_ack {
                    RuusterQueues::track_message_delivery(
                        &mut acks,
                        msg.clone(),
                        DEFAULT_ACK_DURATION,
                    )?;
                }
                log::debug!("consuming single message from queue: {} completed", queue_name);
                Ok(msg)
            }
            None => Err(Status::not_found("failed to return message")),
        }
    }

    pub async fn start_consuming_task(
        &self,
        queue_name: &QueueName,
        auto_ack: bool,
    ) -> ReceiverStream<Result<Message, Status>> {
        log::debug!("spawning consuming task for queue: {}", queue_name);
        let (tx, rx) = mpsc::channel(4);
        let queues = self.queues.clone();
        let queue_name = queue_name.clone();
        let acks_arc = self.acks.clone();

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
                    if !auto_ack {
                        let mut acks = acks_arc.write().unwrap();
                        // TODO(msaff): add proper error handling
                        let _ = RuusterQueues::track_message_delivery(
                            &mut acks,
                            message.clone(),
                            DEFAULT_ACK_DURATION,
                        );
                    }
                   
                    if let Err(e) = tx.send(Ok(message)).await {
                        let msg = format!("error while sending message to channel: {}", e);
                        log::error!("{}", msg);
                        return Status::internal(msg);
                    }
                    log::debug!("message from queue: {} correclty sent over channel", queue_name);
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
        ReceiverStream::new(rx)
    }

    fn log_status(message: &String, code: tonic::Code) -> Status {
        Status::new(code, message)
    }
}