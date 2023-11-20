use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Response};
use tonic::{Request, Status};

use ruuster::ruuster_server::*;
use ruuster::*;

use exchanges::types::*;
use exchanges::*;

pub mod ruuster {
    tonic::include_proto!("ruuster");
}

type Message = ruuster::Message;
type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
}

impl RuusterQueues {
    fn new() -> Self {
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
                return Err(Status::new(tonic::Code::Unimplemented, msg));
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

        match (queues.get(queue_name), excahnges.get(exchange_name)) {
            (Some(_), Some(e)) => {
                let mut e_lock = e.write().unwrap();
                e_lock.bind(queue_name);
            }
            (_, _) => {
                let msg = "binding failed - parsing was unsucessfull";
                log::error!("{}", msg);
                return Err(Status::new(tonic::Code::NotFound, msg));
            }
        };

        log::trace!("bind_queue_to_exchange finished sucessfully");

        Ok(Response::new(Empty {}))
    }

    //NOTICE(msaff): this is not an only option, if it will not be good enough we can always change it to smoething more... lov-level ;)
    type ConsumeStream = ReceiverStream<Result<Message, Status>>;

    /**
     * this request will receive messages from specific queue in form of asynchronous stream
     */
    async fn consume(
        &self,
        _request: tonic::Request<ListenRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let (_tx, rx) = mpsc::channel(4);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /**
     * this request will take messages in form of asynchronous stream and push them into proper exchange
     * exchange will decide what to do with each one of them
     */
    async fn produce(
        &self,
        _request: Request<tonic::Streaming<Message>>,
    ) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();
    let ruuster_queue_service = RuusterQueues::new();

    Server::builder()
        .add_service(RuusterServer::new(ruuster_queue_service))
        .serve(addr)
        .await?;

    Ok(())
}
