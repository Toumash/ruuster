use std::collections::{VecDeque, HashMap};
use std::sync::{Mutex, RwLock, Arc};

use ruuster::{Empty, ListQueuesResponse, ListExchangesResponse};
use tonic::{transport::Server, Response};
use ruuster::{ruuster_server::RuusterServer, QueueDeclareRequest, QueueDeclareResponse, ExchangeDeclareRequest, ExchangeDeclareResponse, BindQueueToExchangeRequest, BindQueueToExchangeResponse, PublishToExchangeRequest, PublishToExchangeResponse};

use exchanges::*;
use exchanges::types::*;

pub mod ruuster {
    tonic::include_proto!("ruuster");
}

type Message = ruuster::MessageBody;
type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>
}

impl RuusterQueues {
    fn new() -> Self {
        RuusterQueues{
            queues: Arc::new(RwLock::new(HashMap::new())),
            exchanges: Arc::new(RwLock::new(HashMap::new()))
        }
    }
}

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueues {
    async fn queue_declare(
        &self, 
        request: tonic::Request<QueueDeclareRequest>
    ) -> Result<tonic::Response<QueueDeclareResponse>, tonic::Status> {
        println!("QueueDeclare");
        let mut queues_lock = self.queues.write().unwrap();
        queues_lock.insert(request.get_ref().queue_name.clone(), Mutex::new(VecDeque::new()));
        Ok(Response::new(QueueDeclareResponse{status_code: 200}))
    }

    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>
    ) -> Result<tonic::Response<ExchangeDeclareResponse>, tonic::Status> {
        println!("ExchangeDeclare");
        let mut exchanges_lock = self.exchanges.write().unwrap();
        let exchange_def = request.get_ref().exchange.clone().unwrap();
        match exchange_def.kind {
            0 => { 
                exchanges_lock.insert(
                    exchange_def.exchange_name, 
                    Arc::new(Mutex::new(FanoutExchange::default())));
                return Ok(Response::new(ExchangeDeclareResponse{status_code: tonic::Code::Ok as u32}));
            }
            other => {
                return Err(
                    tonic::Status::new(
                        tonic::Code::Unimplemented, 
                        format!("Exchange: {} not supported yet ;)", other)
                    )
                );
            }
        };
    }

    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        println!("ListQueues");
        let mut response = ListQueuesResponse::default();
        let queues_lock = self.queues.read().unwrap();
        for queue in queues_lock.iter() {
            response.queue_names.push(queue.0.clone());
        }
        Ok(Response::new(response))
    }

    async fn list_exchanges(
        &self, 
        _request: tonic::Request<Empty>
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        println!("ListExchanges");

        let mut response = ListExchangesResponse::default();
        let exchanges_lock = self.exchanges.read().unwrap();
        for exchange in exchanges_lock.iter() {
            response.exchange_names.push(exchange.0.clone());
        }

        Ok(Response::new(response))
    }

    async fn bind_queue_to_exchange(
        &self,
        request: tonic::Request<BindQueueToExchangeRequest>
    ) -> Result<tonic::Response<BindQueueToExchangeResponse>, tonic::Status> {
        println!("BindQueueToExchange");

        let queue_name = &request.get_ref().queue_name;
        let exchange_name = &request.get_ref().exchange_name;

        let queues = self.queues.read().unwrap();
        let excahnges = self.exchanges.read().unwrap();

        match(queues.get(queue_name), excahnges.get(exchange_name))
        {
            (Some(_), Some(e)) => {
                let mut e_lock = e.lock().unwrap();
                e_lock.bind(queue_name);
                return Ok(
                    Response::new(
                        BindQueueToExchangeResponse{status_code: tonic::Code::Ok as u32}
                        )
                    );
            },
            (_,_) => {
                return Err(
                    tonic::Status::new(
                        tonic::Code::NotFound, 
                        "Binding failed - parsing was unsucessfull"
                    )
                );
            }
        }

    }

    async fn publish_to_exchange(
        &self,
        _request: tonic::Request<PublishToExchangeRequest>
    ) -> Result<tonic::Response<PublishToExchangeResponse>, tonic::Status> {
        println!("PublishToExchange");
        Ok(Response::new(PublishToExchangeResponse{status_code: tonic::Code::Ok as u32}))
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
