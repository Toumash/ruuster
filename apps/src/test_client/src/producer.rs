use std::time::Duration;

use clap::Parser;
use protos::{ruuster_client::RuusterClient, ProduceRequest};
use tonic::transport::Channel;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    server_addr: String,
    #[arg(short, long)]
    destination: String,
    #[arg(short, long)]
    messages_produced: i32,
    #[arg(short, long)]
    message_payload_bytes: i32,
    #[arg(short, long)]
    delay_ms: i32,
}

const STOP_TOKEN: &str = "STOP";

async fn run_producer(args: Args, client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>>
{
    let mut messages_countdown = args.messages_produced;

    while messages_countdown >= 0 {
        let request = ProduceRequest {
            payload: utils::generate_random_string(args.message_payload_bytes.try_into().unwrap()),
            exchange_name: args.destination.clone(),
            metadata: None //TODO: add support for direct exchange
        };
        client.produce(request).await?;

        tokio::time::sleep(Duration::from_millis(args.delay_ms.try_into().unwrap())).await;

        messages_countdown -= 1;
    }
    let stop_request = ProduceRequest {
        payload: STOP_TOKEN.try_into().unwrap(),
        metadata: None, 
        exchange_name: args.destination
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

    Ok(())
}