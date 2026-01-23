//! Ruuster Server CLI
//!
//! Command-line interface for running the Ruuster message broker server.
//! By default, runs on 127.0.0.1:50051

use ruuster_server::{ServerConfig, run_server};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse address from environment variable or use default
    let addr = env::var("RUUSTER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;

    let config = ServerConfig::new(addr);

    run_server(config).await
}
