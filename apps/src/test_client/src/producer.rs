use std::time::Duration;

use crate::config_definition::Metadata;
use clap::Parser;
use protos::{ruuster_client::RuusterClient, ProduceRequest, RoutingKey};
use tonic::transport::Channel;

mod config_definition;

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
    metadata: Option<Metadata>,
}

const STOP_TOKEN: &str = "STOP";

fn parse_metadata(config_metadata: &Option<Metadata>) -> Option<protos::Metadata> {
    match config_metadata {
        None => None,
        Some(meta) => Some(protos::Metadata {
            routing_key: Some(RoutingKey {
                value: meta.routing_key.clone(),
            }),
        }),
    }
}

async fn run_producer(
    args: Args,
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut messages_countdown = args.messages_produced;
    let metadata = args.metadata;
    println!("Producing messages...");
    while messages_countdown >= 0 {
        let payload = messages_countdown.to_string()
            + " "
            + &*utils::generate_random_string(args.message_payload_bytes.try_into().unwrap());
        let request = ProduceRequest {
            payload,
            exchange_name: args.destination.clone(),
            metadata: parse_metadata(&metadata),
        };
        client.produce(request).await?;
        tokio::time::sleep(Duration::from_millis(args.delay_ms.try_into().unwrap())).await;

        messages_countdown -= 1;
    }

    println!("Producing STOP message");
    let stop_request = ProduceRequest {
        payload: STOP_TOKEN.try_into().unwrap(),
        metadata: None,
        exchange_name: args.destination,
    };
    client.produce(stop_request).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut client = RuusterClient::connect(args.server_addr.clone())
        .await
        .expect("failed to create consumer client");

    run_producer(args, &mut client).await?;

    println!("Exiting producer");
    Ok(())
}
