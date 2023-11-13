use std::sync::Mutex;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use mq::{ProduceRequest, ConsumeResponse};
use mq::mq_server::{Mq, MqServer};

pub mod mq {
    tonic::include_proto!("mq");
}

#[derive(Debug, Default)]
pub struct MqService {
    queue: Mutex<Vec<String>>,
}


#[tonic::async_trait]
impl Mq for MqService {
    async fn produce(&self, request: Request<ProduceRequest>) -> Result<Response<()>, Status> {
        let que = self.queue.lock();
        match que {
            Ok(mut vc) => {
                let message = request.into_inner().message;
                println!("Got {message}");
                vc.push(message);
                Ok(Response::new(()))
            },
            Err(e) => Err(Status::new(tonic::Code::Unavailable, e.to_string()))
         }
    }

    #[allow(unused_variables)]
    async fn consume(&self, request: Request<()>) -> Result<Response<ConsumeResponse>, Status> {
        let mut locked_queue = self.queue.lock().unwrap();
        match locked_queue.pop() {
            Some(val) => Ok(Response::new(ConsumeResponse { message: val })),
            None => Err(Status::new(tonic::Code::Unavailable, "Fucked up".to_string()))
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    let addr = "[::1]:50051".parse()?;
    let mq_service = MqService::default();
    
    Server::builder()
        .add_service(MqServer::new(mq_service))
        .serve(addr)
        .await?;

    Ok(())
}