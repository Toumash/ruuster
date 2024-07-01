use std::time::Duration;

use clap::Parser;
use protos::{ruuster_client::RuusterClient, ProduceRequest};
use tonic::transport::Channel;
use tracing::info;

mod config_definition;
mod metadata_parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    server_addr: String,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    messages_produced: i32,
    #[arg(long)]
    message_payload_bytes: i32,
    #[arg(long)]
    delay_ms: i32,
    #[arg(long)]
    metadata: Option<String>,
}

async fn run_producer(
    args: Args,
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut messages_countdown = args.messages_produced;
    let metadata_arg = args.metadata;
    info!(messages_to_produce=%messages_countdown, "producing messages");
    while messages_countdown > 0 {
        let payload = messages_countdown.to_string()
            + " "
            + &*utils::generate_random_string(args.message_payload_bytes.try_into().unwrap());
        let request = ProduceRequest {
            payload,
            exchange_name: args.destination.clone(),
            metadata: metadata_parser::parse_metadata(&metadata_arg),
        };
        client.produce(request).await?;
        // debug!(message=%request, "produce request");
        tokio::time::sleep(Duration::from_millis(args.delay_ms.try_into().unwrap())).await;

        messages_countdown -= 1;
    }

    info!("produced all messages");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to setup producer logs");
    
    let args = Args::parse();

    let mut client = RuusterClient::connect(args.server_addr.clone())
        .await
        .expect("failed to create consumer client");

    run_producer(args, &mut client).await?;

    info!("exiting producer");
    Ok(())
}
