use std::collections::HashMap;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::{mpsc, Notify};
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;

use exchanges::types::*;
use exchanges::*;

use protos::ruuster;
use protos::*;

type Uuid = String;
type AckContainer = HashMap<Uuid, Arc<(Notify, AtomicU32)>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
    acks: Arc<RwLock<AckContainer>>,
}

impl RuusterQueues {
    pub fn new() -> Self {
        RuusterQueues {
            queues: Arc::new(RwLock::new(QueueContainer::new())),
            exchanges: Arc::new(RwLock::new(ExchangeContainer::new())),
            acks: Arc::new(RwLock::new(AckContainer::new())),
        }
    }
}

impl Default for RuusterQueues {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueues {
    async fn queue_declare(
        &self,
        request: tonic::Request<QueueDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let queue_name = request.get_ref().queue_name.clone();
        let mut queues_lock = self.queues.write().unwrap();
        if queues_lock.get(&queue_name).is_some() {
            let msg = format!("queue({}) already exists", queue_name.clone());
            log::error!("{}", msg);
            return Err(Status::already_exists(msg));
        }
        // TODO: make the queue size configureable (rabbit - size  policy)
        // see: https://www.rabbitmq.com/maxlength.html
        queues_lock.insert(queue_name, Mutex::new(Queue::new()));
        log::trace!("queue declare finished successfully");
        Ok(Response::new(Empty {}))
    }

    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let mut exchanges_lock = self.exchanges.write().unwrap();
        let exchange_def = request.get_ref().exchange.clone().unwrap();
        if exchanges_lock.get(&exchange_def.exchange_name).is_some() {
            let msg = format!(
                "exchange({}) already exists",
                exchange_def.exchange_name.clone()
            );
            log::error!("{}", msg);
            return Err(Status::already_exists(msg));
        }
        match exchange_def.kind {
            0 => {
                exchanges_lock.insert(
                    exchange_def.exchange_name,
                    Arc::new(RwLock::new(FanoutExchange::default())),
                );
            }
            other => {
                let msg = format!("exchange({}) not supported yet", other);
                log::error!("{}", msg);
                return Err(Status::unimplemented(msg));
            }
        };

        log::trace!("exchange_declare finished successfully");
        Ok(Response::new(Empty {}))
    }

    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        let queues_lock = self.queues.read().unwrap();
        let response = ListQueuesResponse {
            queue_names: queues_lock.iter().map(|queue| queue.0.clone()).collect(),
        };
        log::trace!("list_queues finished sucessfully");
        Ok(Response::new(response))
    }

    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        let exchanges_lock = self.exchanges.read().unwrap();
        let response = ListExchangesResponse {
            exchange_names: exchanges_lock
                .iter()
                .map(|exchange| exchange.0.clone())
                .collect(),
        };
        log::trace!("list_exchanges finished sucessfully");
        Ok(Response::new(response))
    }

    async fn bind_queue_to_exchange(
        &self,
        request: tonic::Request<BindQueueToExchangeRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let queue_name = &request.get_ref().queue_name;
        let exchange_name = &request.get_ref().exchange_name;

        let queues = self.queues.read().unwrap();
        let excahnges = self.exchanges.read().unwrap();

        let mut exchange_write = match (queues.get(queue_name), excahnges.get(exchange_name)) {
            (Some(_), Some(e)) => e.write().unwrap(),
            (_, _) => {
                let msg = "binding failed: requested queue or exchange doesn't exists";
                log::error!("{}", msg);
                return Err(Status::not_found(msg));
            }
        };

        match exchange_write.bind(queue_name) {
            Ok(()) => {
                log::trace!("bind_queue_to_exchange finished sucessfully");
                return Ok(Response::new(Empty {}));
            }
            Err(e) => {
                let msg = format!("binding failed: {}", e);
                log::error!("{}", msg);
                return Err(Status::internal(msg));
            }
        }
    }

    //NOTICE(msaff): this is not an only option, if it will not be good enough we can always change it to smoething more... lov-level ;)
    type ConsumeStream = ReceiverStream<Result<Message, Status>>;

    /**
     * this request will receive messages from specific queue and return it in form of asynchronous stream
     */
    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let queue_name = request.into_inner().queue_name;
        let (tx, rx) = mpsc::channel(4);
        let queues = self.queues.clone();

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

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn consume_one(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Message>, Status> {
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

        Ok(Response::new(message))
    }

    /**
     * for now let's have a simple producer that will push message into exchange requested in message header
     */
    async fn produce(
        &self,
        request: tonic::Request<ProduceRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let exchange_name = req.exchange_name;

        let payload = &req.payload;
        if payload.is_none() {
            let msg = "payload is empty";
            log::warn!("{}", msg);
            return Err(Status::failed_precondition(msg));
        }

        let uuid = &payload.as_ref().unwrap().uuid;

        let requested_exchange = {
            let exchanges_read = self.exchanges.read().unwrap();

            let result = match exchanges_read.get(&exchange_name) {
                Some(exchange) => exchange,
                None => return Err(Status::not_found("requested exchange doesn't exist")),
            };
            result.clone()
        };

        let handling_result = {
            let requested_exchange_read = requested_exchange.read().unwrap();
            requested_exchange_read.handle_message(&req.payload, self.queues.clone())
        };

        if let Err(e) = handling_result {
            let msg = format!("message handling failed: {}", e);
            log::error!("{}", msg);
            return Err(Status::internal(msg));
        } else {
            log::info!(
                "message with id: {} sent to exchange: {}",
                &uuid,
                &exchange_name
            );
        }

        let uuid = req.payload.unwrap().uuid;
        let acks_arc = self.acks.clone();
        let ack_notification = {
            let mut acks_write = acks_arc.write().unwrap();

            acks_write
                .entry(uuid.clone())
                .or_insert(Arc::new((
                    Notify::new(),
                    AtomicU32::new(handling_result.unwrap()),
                )))
                .clone()
        };

        let ack_future = async move {
            let timeout_future = timeout(tokio::time::Duration::from_secs(10), async move {
                loop {
                    let flag = &ack_notification.0;
                    flag.notified().await;
                    let counter = &ack_notification.1;
                    log::info!("message {} akcnowledged", &uuid);
                    //let's check if ack_notification gathered all acknowledgment calls
                    if counter.fetch_sub(1, Ordering::SeqCst) > 1 {
                        continue; // this message have to be acknowledged more times
                    }

                    // counter has value 0 so we can safely remove notification entry from container
                    let mut acks_write = acks_arc.write().unwrap();
                    match acks_write.remove(&uuid) {
                        Some(_) => {
                            log::info!("ack notifier for {} removed", &uuid);
                            break;
                        }
                        None => {
                            log::warn!("ack notifier for {} already deleted", &uuid);
                            break;
                        }
                    };
                }
            });

            match timeout_future.await {
                Ok(_) => return,
                Err(e) => log::error!("ack error: {}", e),
            }
        };
        tokio::spawn(ack_future);

        Ok(Response::new(Empty {}))
    }

    async fn ack_message(
        &self,
        request: tonic::Request<AckRequest>,
    ) -> Result<Response<Empty>, Status> {
        log::trace!("ack_message: {:#?}", request);
        let requested_uuid = &request.into_inner().uuid;
        let acks_read = self.acks.read().unwrap();
        let ack_flag_option = acks_read.get(requested_uuid);

        if ack_flag_option.is_none() {
            let msg = format!(
                "message: {} requested for ack doesn't exist",
                requested_uuid
            );
            log::error!("{}", msg);
            return Err(Status::not_found(msg));
        }

        let ack_flag = ack_flag_option.unwrap();
        ack_flag.0.notify_one();

        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests_utils;

#[cfg(test)]
mod tests;
