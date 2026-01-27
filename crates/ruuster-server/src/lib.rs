//! Ruuster Server - A high-performance message broker gRPC server
//!
//! This library provides the core server implementation for the Ruuster message broker.
//! It can be used as a library for embedding the broker in other applications,
//! or run standalone via the binary.

mod domain;
mod error;
mod server;
mod services;

pub use server::RuusterServer;

use ruuster_core::Queue;
use ruuster_protos::v1::ack_service_server::AckServiceServer;
use ruuster_protos::v1::message_service_server::MessageServiceServer;
use ruuster_protos::v1::topology_service_server::TopologyServiceServer;
use ruuster_router::{DirectStrategy, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

/// Configuration for the Ruuster server
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: SocketAddr,
    pub enable_reflection: bool,
}

// pub struct BrokerConfig {
//     pub max_queues: usize,
//     pub max_connections: usize, // Add ack timeout, etc as needed
// }

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:50051".parse().unwrap(),
            enable_reflection: true,
        }
    }
}

impl ServerConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            enable_reflection: true,
        }
    }

    pub fn with_reflection(mut self, enable: bool) -> Self {
        self.enable_reflection = enable;
        self
    }
}

/// Initialize a router with default topology
/// This sets up a basic exchange and queue configuration for testing/demo purposes
pub fn setup_default_topology(router: &Arc<Router>) {
    router.add_exchange("default", Box::new(DirectStrategy));
    let default_q = Arc::new(Queue::new("default_q".into(), 1000));
    router.add_queue(Arc::clone(&default_q));

    if let Some(ex) = router.get_exchange("default") {
        ex.bind(default_q);
    }
}

/// Run the Ruuster server with the given configuration
pub async fn run_server(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let router = Arc::new(Router::new());
    setup_default_topology(&router);

    run_server_with_router(config, router).await
}

/// Run the Ruuster server with a custom router configuration
/// This allows full control over the broker topology
pub async fn run_server_with_router(
    config: ServerConfig,
    router: Arc<Router>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ruuster_service = Arc::new(RuusterServer::new(router));
    let mut server_builder = Server::builder();

    let message_service = MessageServiceServer::from_arc(ruuster_service.clone());
    let topology_service = TopologyServiceServer::from_arc(ruuster_service.clone());
    let ack_service = AckServiceServer::from_arc(ruuster_service.clone());

    if config.enable_reflection {
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(ruuster_protos::v1::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        println!(
            "🚀 Ruuster Broker started on {} (with reflection)",
            config.addr
        );

        server_builder
            .add_service(message_service)
            .add_service(topology_service)
            .add_service(ack_service)
            .add_service(reflection_service)
            .serve(config.addr)
            .await?;
    } else {
        println!("🚀 Ruuster Broker started on {}", config.addr);

        server_builder
            .add_service(message_service)
            .add_service(topology_service)
            .add_service(ack_service)
            .serve(config.addr)
            .await?;
    }

    Ok(())
}
