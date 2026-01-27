//! Ruuster Server CLI
//!
//! Command-line interface for running the Ruuster message broker server.
//! By default, runs on 127.0.0.1:50051 with reflection enabled.

use ruuster_server::{run_server, ServerConfig};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse address from environment variable or use default
    let addr = env::var("RUUSTER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    let config = ServerConfig::new(addr).with_reflection(true);

    println!("Starting Ruuster Message Broker...");
    println!("Listening on: {}", config.addr);
    println!("gRPC Reflection: {}", if config.enable_reflection { "enabled" } else { "disabled" });

    run_server(config).await
}
