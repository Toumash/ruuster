use std::fs;

use protos::ruuster_server::RuusterServer;

use ruuster_grpc::queues::RuusterQueues;
use tonic::transport::Server;

const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: &str = "50051";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = format!("{}:{}", SERVER_IP, SERVER_PORT).parse().unwrap();
    let ruuster_queue_service = RuusterQueues::new();
    let current_dir = std::env::current_dir()?;
    let ruuster_descriptor_path = current_dir
        .join("protos")
        .join("src")
        .join("ruuster_descriptor.bin");

    let ruuster_descriptor_content = fs::read(ruuster_descriptor_path)?;

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(&ruuster_descriptor_content)
        .build()?;

    log::info!("Starting server on address: {}", &addr);

    Server::builder()
        .add_service(RuusterServer::new(ruuster_queue_service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
