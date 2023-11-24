use ruuster_grpc::RuusterQueues;
use protos::ruuster_server::RuusterServer;

use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    log::info!("Welcome!");
    let addr = "127.0.0.1:50051".parse().unwrap();
    let ruuster_queue_service = RuusterQueues::new();

    Server::builder()
        .add_service(RuusterServer::new(ruuster_queue_service))
        .serve(addr)
        .await?;

    Ok(())
}
