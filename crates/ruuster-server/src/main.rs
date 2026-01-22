use ruuster_core::Queue;
use ruuster_protos::v1::ruuster_service_server::RuusterServiceServer;
use ruuster_router::{DirectStrategy, Router};
use std::sync::Arc;
use tonic::transport::Server;

mod service;
use service::RuusterServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let router = Arc::new(Router::new());

    // --- SETUP TOPOLOGY ---
    router.declare_exchange("default", Box::new(DirectStrategy));
    let default_q = Arc::new(Queue::new("default_q".into(), 1000));
    router.add_queue(Arc::clone(&default_q));

    if let Some(ex) = router.get_exchange("default") {
        ex.bind(default_q);
    }

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(ruuster_protos::v1::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let ruuster_service = RuusterServer::new(router);

    println!("🚀 Ruuster Broker started on {}", addr);

    Server::builder()
        .add_service(RuusterServiceServer::new(ruuster_service))
        .add_service(reflection_service) // Add this line!
        .serve(addr)
        .await?;

    Ok(())
}
