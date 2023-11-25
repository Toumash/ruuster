use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::Response;

use exchanges::types::*;
use exchanges::*;

use protos::*;
use protos::ruuster;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
}

impl RuusterQueues {
    pub fn new() -> Self {
        RuusterQueues {
            queues: Arc::new(RwLock::new(HashMap::new())),
            exchanges: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueues {
    async fn queue_declare(
        &self,
        request: tonic::Request<QueueDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        log::trace!("started queue_declare");
        let mut queues_lock = self.queues.write().unwrap();
        queues_lock.insert(
            request.get_ref().queue_name.clone(),
            Mutex::new(VecDeque::new()),
        );
        log::trace!("queue declare finished successfully");
        Ok(Response::new(Empty {}))
    }

    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        log::trace!("started queue_declare");
        let mut exchanges_lock = self.exchanges.write().unwrap();
        let exchange_def = request.get_ref().exchange.clone().unwrap();
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
        log::trace!("started list_queues");
        let queues_lock = self.queues.read().unwrap();
        let response = ListQueuesResponse {
            queue_names: queues_lock.iter().map(|queue| queue.0.clone()).collect(),
            ..ListQueuesResponse::default()
        };
        log::trace!("list_queues finished sucessfully");
        Ok(Response::new(response))
    }

    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        log::trace!("started list_exchanges");

        let exchanges_lock = self.exchanges.read().unwrap();
        let response = ListExchangesResponse {
            exchange_names: exchanges_lock
                .iter()
                .map(|exchange| exchange.0.clone())
                .collect(),
            ..ListExchangesResponse::default()
        };
        log::trace!("list_exchanges finished sucessfully");
        Ok(Response::new(response))
    }

    async fn bind_queue_to_exchange(
        &self,
        request: tonic::Request<BindQueueToExchangeRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        log::trace!("started bind_queue_to_exchange");

        let queue_name = &request.get_ref().queue_name;
        let exchange_name = &request.get_ref().exchange_name;

        let queues = self.queues.read().unwrap();
        let excahnges = self.exchanges.read().unwrap();

        let mut exchange_write = match (queues.get(queue_name), excahnges.get(exchange_name)) {
            (Some(_), Some(e)) => {
                e.write().unwrap()
            }
            (_, _) => {
                let msg = "binding failed: requested queue or exchange doesn't exists";
                log::error!("{}", msg);
                return Err(Status::not_found(msg));
            }
        };

        match exchange_write.bind(queue_name)
        {
            Ok(()) => {
                log::trace!("bind_queue_to_exchange finished sucessfully");
                return Ok(Response::new(Empty {}));
            },
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
        request: tonic::Request<ListenRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        
        let queue_name = request.into_inner().queue_name;
        let (tx, rx) = mpsc::channel(4);
        let queues = self.queues.clone();

        tokio::spawn(async move {
            loop {
                let message = {
                    let queues_read = queues.read().unwrap();
    
                    let queue_arc = match queues_read.get(&queue_name) {
                        Some(queue) => queue,
                        None => {
                            eprintln!("requested queue doesn't exist");
                            return;
                        }
                    };
    
                    let mut queue = queue_arc.lock().unwrap();
                    queue.pop_front()
                };

                if let Some(message) = message {
                    if tx.send(Ok(message)).await.is_err() {
                        break;
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /**
     * for now let's have a simple producer that will push message into exchange requested in message header
     */
    async fn produce(&self, request: tonic::Request<Message>) -> Result<Response<Empty>, Status> {
        log::trace!("start produce");

        let msg = request.into_inner();

        let exchange_name = match &msg.header {
            Some(h) => h.exchange_name.clone(),
            None => return Err(Status::invalid_argument("message header is missing")),
        };

        let exchanges_read = self.exchanges.read().unwrap();

        let requested_exchange = match exchanges_read.get(&exchange_name) {
            Some(exchange) => exchange,
            None => return Err(Status::not_found("requested exchange doesn't exist")),
        };

        let requested_exchange_read = requested_exchange.read().unwrap();
        
        match requested_exchange_read.handle_message(&msg, self.queues.clone()) {
            Ok(()) => log::trace!("message sent"),
            Err(e) => log::error!("error occured while handling message: {}", e)
        };
        

        Ok(Response::new(Empty {}))
    }
}