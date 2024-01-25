use protos::ruuster_server::RuusterServer;

use ruuster_grpc::queues::RuusterQueues;
use tonic::transport::Server;

const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: &str = "50051";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    log::info!("Welcome!");
    let addr = format!("{}:{}", SERVER_IP, SERVER_PORT).parse().unwrap();
    let ruuster_queue_service = RuusterQueues::new();

    Server::builder()
        .add_service(RuusterServer::new(ruuster_queue_service))
        .serve(addr)
        .await?;

    Ok(())
}
