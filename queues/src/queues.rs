use exchanges::{ExchangeContainer, ExchangeKind, ExchangeName, ExchangeType};

use internals::{Message, Metadata};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{self, error, info, info_span, instrument, warn, Instrument};
use uuid::Uuid;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};

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

    // NOTICE: for some reason function parameters must be explicitly specified
    //         eg.: #[instrument(skip(self))] should work exactly the same
    //         but filtering by tag is broken then, even tho queue_name is properly added to tags of span
    //         I suspect that it is some kind of bug on Jaeger side
    #[instrument(skip_all, fields(queue_name=%queue_name))]
    pub fn add_queue(&self, queue_name: &QueueName) -> Result<(), Status> {
        info!("adding queue");
        let mut queues_write = self.queues.write().map_err(|e| {
            error!(error=%e, "queues are unavailable");
            Status::unavailable("queues are unavailable")
        })?;

        if queues_write.get(queue_name).is_some() {
            warn!("queue already exists");
            return Err(Status::already_exists("queue already exists"));
        }

        queues_write.insert(queue_name.to_owned(), Arc::new(Mutex::new(VecDeque::new())));

        info!("queue added");
        Ok(())
    }

    #[instrument(skip_all, fields(queue_name=%queue_name))]
    pub fn remove_queue(&self, queue_name: &QueueName) -> Result<(), Status> {
        info!("removing queue");
        let mut queues_write = self.queues.write().map_err(|e| {
            error!(error=%e, "queues are unavailable");
            Status::unavailable("queues are unavailable")
        })?;

        if queues_write.remove(queue_name).is_none() {
            warn!("queue does not exist");
            return Err(Status::not_found("queue does not exist"));
        }

        info!("queue removed");
        Ok(())
    }

    #[instrument(skip_all, fields(queue_name=%queue_name))]
    pub fn get_queue(&self, queue_name: &QueueName) -> Result<Arc<Mutex<Queue>>, Status> {
        let queues_read = self.queues.read().map_err(|e| {
            error!(error=%e, "queue is unavailable");
            Status::unavailable("queues is unavailable")
        })?;

        match queues_read.get(queue_name) {
            Some(queue) => Ok(queue.clone()),
            None => {
                error!("queue not found");
                Err(Status::not_found("queue not found"))
            }
        }
    }

    #[instrument(skip_all, fields(exchange_name=%exchange_name, exchange_kind=%exchange_kind))]
    pub fn add_exchange(
        &self,
        exchange_name: &ExchangeName,
        exchange_kind: ExchangeKind,
    ) -> Result<(), Status> {
        info!("adding exchange");
        let mut exchanges_write = self.exchanges.write().map_err(|e| {
            error!(error=%e, "exchanges are unavailable");
            Status::unavailable("exchanges are unavailable")
        })?;

        if exchanges_write.get(exchange_name).is_some() {
            warn!("exchange already exists");
            return Err(Status::already_exists("exchange already exists"));
        }

        exchanges_write.insert(exchange_name.to_owned(), exchange_kind.create());

        info!("exchange added");
        Ok(())
    }

    #[instrument(skip_all, fields(exchange_name=%exchange_name))]
    pub fn remove_exchange(&self, exchange_name: &ExchangeName) -> Result<(), Status> {
        let mut exchanges_write = self.exchanges.write().map_err(|e| {
            error!(error=%e, "exchanges are unavailable");
            Status::unavailable("exchanges are unavailable")
        })?;

        if exchanges_write.remove(exchange_name).is_none() {
            warn!("exchange does not exist");
            return Err(Status::not_found("exchange does not exist"));
        }
        Ok(())
    }

    #[instrument(skip_all, fields(exchange_name=%exchange_name))]
    pub fn get_exchange(
        &self,
        exchange_name: &ExchangeName,
    ) -> Result<Arc<RwLock<ExchangeType>>, Status> {
        let exchanges_read = self.exchanges.read().map_err(|e| {
            error!(error=%e, "exchange is unavailable");
            Status::unavailable("exchange is unavailable")
        })?;

        match exchanges_read.get(exchange_name) {
            Some(exchange) => Ok(exchange.clone()),
            None => {
                error!("exchange not found");
                Err(Status::not_found("exchange not found"))
            }
        }
    }

    #[instrument(skip_all)]
    pub fn get_queues_list(&self) -> Result<Vec<QueueName>, Status> {
        info!("gathering queues list");
        let queues_read = self.queues.read().map_err(|e| {
            error!(error=%e, "queues are unavailable");
            Status::unavailable("queues are unavaiable")
        })?;
        let result = queues_read.iter().map(|queue| queue.0.clone()).collect();
        info!("queues list gathered correctly");
        Ok(result)
    }

    #[instrument(skip_all)]
    pub fn get_exchanges_list(&self) -> Result<Vec<ExchangeName>, Status> {
        info!("gathering exchanges list");
        let exchanges_read = self.exchanges.read().map_err(|e| {
            error!(error=%e, "exchanges are unavaiable");
            Status::unavailable("exchanges are unavaiable")
        })?;
        let result = exchanges_read
            .iter()
            .map(|exchange| exchange.0.clone())
            .collect();
        info!("exchanges list gathered correctly");
        Ok(result)
    }

    #[instrument(skip_all, fields(exchange_name=%exchange_name, queue_name=%queue_name))]
    pub fn unbind_queue_from_exchange(
        &self,
        queue_name: &QueueName,
        exchange_name: &ExchangeName,
        metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), Status> {
        let exchange = self.get_exchange(exchange_name)?;
        let mut exchange_write = exchange.write().map_err(|e| {
            error!(error=%e, "failed to accuire exchange lock for writting");
            Status::unavailable("exchange is unavaiable")
        })?;
        exchange_write.unbind(queue_name, metadata).map_err(|e| {
            error!(error=%e, "failed to unbind exchange");
            Status::internal("failed to unbind exchange")
        })?;
        Ok(())
    }

    pub fn bind_queue_to_exchange(
        &self,
        queue_name: &QueueName,
        exchange_name: &ExchangeName,
        metadata: Option<&protos::BindMetadata>,
    ) -> Result<(), Status> {
        let _span = match &metadata {
            Some(md) => info_span!(
                "bind_queue_to_exchange",
                queue_name = %queue_name,
                exchange_name=%exchange_name,
                metadata=?md
            )
            .entered(),
            None => info_span!(
                "bind_queue_to_exchange",
                queue_name=%queue_name,
                exchange_name=%exchange_name
            )
            .entered(),
        };

        info!("started binding");
        let exchange = self.get_exchange(exchange_name)?;
        let mut exchange_write = exchange.write().map_err(|e| {
            error!(error=%e, "exchange is unavaiable");
            Status::unavailable("exchange is unavaiable")
        })?;
        exchange_write.bind(queue_name, metadata).map_err(|e| {
            error!(error=%e, "failed to bind");
            Status::internal("failed to bind")
        })?;
        info!("binding completed");
        Ok(())
    }

    pub fn forward_message(
        &self,
        payload: Payload,
        exchange_name: &ExchangeName,
        metadata: Option<Metadata>,
    ) -> Result<u32, Status> {
        let uuid_str = Uuid::new_v4().to_string();

        let _span = match &metadata {
            Some(md) => info_span!(
                "forward_message",
                uuid = %uuid_str,
                exchange_name=%exchange_name,
                metadata=?md
            )
            .entered(),
            None => info_span!(
                "forward_message",
                uuid = %uuid_str,
                exchange_name=%exchange_name
            )
            .entered(),
        };

        info!("creating message");
        let exchange = self.get_exchange(exchange_name)?;

        let result = {
            let exchange_read = exchange.read().map_err(|e| {
                error!(error=%e, "exchange is unavaiable");
                Status::unavailable("exchange is unavaiable")
            })?;

            let message = Message {
                uuid: uuid_str.clone(),
                payload,
                metadata: metadata.or(Some(Metadata {
                    created_at: Some(Instant::now()),
                    routing_key: None,
                    dead_letter: None,
                })),
            };

            exchange_read
                .handle_message(message, self.queues.clone())
                .map_err(|e| {
                    error!(error=%e, "failed to handle message");
                    Status::internal("failed to handle message")
                })
        };
        info!("message handling completed");
        result
    }

    fn get_acks(&self) -> Result<RwLockWriteGuard<'_, AckContainer>, Status> {
        let acks = self.acks.write().map_err(|e| {
            error!(error=%e, "acks are unavaiable");
            Status::unavailable("acks are unavaiable")
        })?;

        Ok(acks)
    }

    #[instrument(skip_all, fields(uuid = %uuid))]
    pub fn apply_message_ack(&self, uuid: UuidSerialized) -> Result<(), Status> {
        info!("started single message ack");
        let mut acks = self.get_acks()?;

        acks.apply_ack(&uuid)?;
        acks.clear_unused_record(&uuid)?;
        info!("ack completed");
        Ok(())
    }

    #[instrument(skip_all)]
    pub fn apply_message_bulk_ack(&self, uuids: &[UuidSerialized]) -> Result<(), Status> {
        info!("acking multiple messages");
        let mut acks = self.get_acks()?;
        acks.apply_bulk_ack(uuids)?;
        acks.clear_all_unused_records()?;
        info!(
            "acking multiple messages completed, acked uuids: {:#?}",
            uuids
        );
        Ok(())
    }

    #[instrument(skip_all, fields(uuid=%message.uuid, duration=?duration))]
    fn track_message_delivery(
        acks: &mut AckContainer,
        message: Message,
        duration: Duration,
    ) -> Result<(), Status> {
        let _enter = tracing::info_span!("track_message_delivery").entered();
        info!("started tracking delivery");
        acks.add_record(message, duration);
        Ok(())
    }

    #[instrument(skip_all, fields(queue_name=%queue_name, auto_ack=%auto_ack))]
    pub fn consume_message(
        &self,
        queue_name: &QueueName,
        auto_ack: bool,
    ) -> Result<Message, Status> {
        info!("started consuming single message");
        let queue = self.get_queue(queue_name)?;
        let mut acks = self.get_acks()?;

        let message = {
            queue
                .lock()
                .map_err(|e| {
                    error!(error=%e, "queue is unavaiable");
                    Status::unavailable("queue is unavaiable")
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
                info!(uuid = &msg.uuid, "consuming single message completed");
                Ok(msg)
            }
            None => return Err(Status::not_found("failed to return message")),
        }
    }

    pub async fn start_consuming_task(
        &self,
        queue_name: &QueueName,
        auto_ack: bool,
    ) -> ReceiverStream<Result<protos::Message, Status>> {
        let (tx, rx) = mpsc::channel(4);
        let queues = self.queues.clone();
        let queue_name = queue_name.clone();
        let acks_arc = self.acks.clone();

        let span = info_span!(
            "start_consuming_task",
            queue_name=%queue_name,
            auto_ack=%auto_ack
        );
        span.in_scope(|| {
            info!("spawning consuming task");
        });

        tokio::spawn(
            async move {
                loop {
                    let message: Option<Message> = {
                        let queues_read = queues.read().unwrap();

                        let requested_queue = match queues_read.get(&queue_name) {
                            Some(queue) => queue,
                            None => {
                                let msg = "requested queue doesn't exist";
                                error!("{}", msg);
                                return Status::not_found(msg);
                            }
                        };

                        let mut queue = requested_queue.lock().unwrap();
                        queue.pop_front()
                    };

                    if let Some(message) = message {
                        if !auto_ack {
                            let mut acks = acks_arc.write().unwrap();
                            match RuusterQueues::track_message_delivery(
                                &mut acks,
                                message.clone(),
                                DEFAULT_ACK_DURATION,
                            ) {
                                Ok(_) => {}
                                Err(status) => {
                                    error!(status=%status, "track message delivery failed");
                                }
                            }
                        }

                        let proto_message = protos::Message::from(message);
                        if let Err(e) = tx.send(Ok(proto_message)).await {
                            let msg = "error while sending message to channel";
                            error!(error=%e, "{}", msg);
                            return Status::internal(msg);
                        }
                        info!("message correctly sent over channel");
                    } else {
                        // tokio::task::yield_now().await;
                        // NOTICE(msaff): yield_now().await re-add task as a pending task at the back of the pending queue
                        //                resulting in 100% usage of 1 logical core, I added a temporary solution with sleep function
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            .instrument(span),
        );
        ReceiverStream::new(rx)
    }
}
