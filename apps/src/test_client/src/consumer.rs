use std::cmp::PartialEq;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use protos::{ruuster_client::RuusterClient, AckMessageBulkRequest, AckRequest, ConsumeRequest};
use rand::Rng;
use tonic::{async_trait, transport::Channel};

#[derive(Clone, ValueEnum, Debug)]
enum ConsumingMethod {
    Single,
    Stream,
}

#[derive(Clone, ValueEnum, Debug, PartialEq)]
enum AckMethod {
    Auto,
    Single,
    Bulk,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    server_addr: String,

    #[arg(long)]
    source: String,

    #[arg(long)]
    consuming_method: ConsumingMethod,

    #[arg(long)]
    ack_method: AckMethod,

    #[arg(long, default_value_t = 0)]
    min_delay_ms: i32,

    #[arg(long, default_value_t = 0)]
    max_delay_ms: i32,
}

// 2 strategies - consume and ack, consume will call a chosen ack method based on a terminal parameter

type UuidSerialized = String;

const STOP_TOKEN: &str = "STOP";
const BULK_ACK_AMOUNT: usize = 20;

#[derive(PartialEq)]
enum AckMode {
    NORMAL,
    FORCE,
}

#[async_trait]
trait AckMethodStrategy {
    async fn acknowledge(
        &mut self,
        client: &mut RuusterClient<Channel>,
        uuid: &UuidSerialized,
        mode: AckMode,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn is_auto(&self) -> bool;
}

#[derive(Default)]
struct AutoAckMethod;

#[async_trait]
impl AckMethodStrategy for AutoAckMethod {
    async fn acknowledge(
        &mut self,
        _client: &mut RuusterClient<Channel>,
        _uuid: &UuidSerialized,
        _ack_mode: AckMode,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn is_auto(&self) -> bool {
        true
    }
}

#[derive(Default)]
struct SingleAckMethod;

#[async_trait]
impl AckMethodStrategy for SingleAckMethod {
    async fn acknowledge(
        &mut self,
        client: &mut RuusterClient<Channel>,
        uuid: &UuidSerialized,
        _mode: AckMode,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = AckRequest {
            uuid: uuid.to_owned(),
        };

        client.ack_message(request).await?;

        Ok(())
    }

    fn is_auto(&self) -> bool {
        false
    }
}

#[derive(Default)]
struct BulkAckMethod {
    uuids: Vec<UuidSerialized>,
}

#[async_trait]
impl AckMethodStrategy for BulkAckMethod {
    async fn acknowledge(
        &mut self,
        client: &mut RuusterClient<Channel>,
        uuid: &UuidSerialized,
        mode: AckMode,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if mode == AckMode::FORCE {
            let request = AckMessageBulkRequest {
                uuids: self.uuids.clone(),
            };
            client.ack_message_bulk(request).await?;
            return Ok(());
        }

        self.uuids.push(uuid.to_owned());

        if self.uuids.len() >= BULK_ACK_AMOUNT {
            let request = AckMessageBulkRequest {
                uuids: self.uuids.clone(),
            };
            client.ack_message_bulk(request).await?;
            println!("acked: {:#?}", &self.uuids);
            self.uuids.clear();
        }
        Ok(())
    }

    fn is_auto(&self) -> bool {
        false
    }
}

#[async_trait]
trait ConsumingMethodStrategy {
    async fn consume(
        &mut self,
        client: &mut RuusterClient<Channel>,
        args: Args,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

struct StreamConsumingMethod {
    queue_name: String,
    ack_method: Box<dyn AckMethodStrategy + Send + Sync>,
}

impl StreamConsumingMethod {
    fn new(queue_name: String, ack_method: Box<dyn AckMethodStrategy + Send + Sync>) -> Self {
        StreamConsumingMethod {
            queue_name,
            ack_method,
        }
    }
}

#[async_trait]
impl ConsumingMethodStrategy for StreamConsumingMethod {
    async fn consume(
        &mut self,
        client: &mut RuusterClient<Channel>,
        args: Args,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = ConsumeRequest {
            queue_name: self.queue_name.clone(),
            auto_ack: self.ack_method.is_auto(),
        };

        let delay = (args.min_delay_ms, args.max_delay_ms);

        let mut response_stream = client.consume_bulk(request).await?.into_inner();
        println!("Consuming...");
        while let Some(message) = response_stream.message().await? {
            // simulate workload
            let workload_ms = {
                let mut rng = rand::thread_rng();
                rng.gen_range(delay.0..=delay.1) as u64
            };
            tokio::time::sleep(Duration::from_millis(workload_ms)).await;
            // println!("Consumed payload {:.20}...", &message.payload);

            self.ack_method
                .acknowledge(client, &message.uuid, AckMode::NORMAL)
                .await?;

            if message.payload == STOP_TOKEN {
                println!("Received stop token");
                self.ack_method
                    .acknowledge(client, &message.uuid, AckMode::FORCE)
                    .await?;
                break;
            }
        }
        println!("End of consuming");
        Ok(())
    }
}

struct SingleConsumingMethod {
    queue_name: String,
    ack_method: Box<dyn AckMethodStrategy + Send + Sync>,
}

impl SingleConsumingMethod {
    fn new(queue_name: String, ack_method: Box<dyn AckMethodStrategy + Send + Sync>) -> Self {
        SingleConsumingMethod {
            queue_name,
            ack_method,
        }
    }
}

#[async_trait]
impl ConsumingMethodStrategy for SingleConsumingMethod {
    async fn consume(
        &mut self,
        client: &mut RuusterClient<Channel>,
        args: Args,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let delay = (args.min_delay_ms, args.max_delay_ms);
        println!("Consuming...");
        loop {
            let request = ConsumeRequest {
                queue_name: self.queue_name.clone(),
                auto_ack: self.ack_method.is_auto(),
            };

            let response = client.consume_one(request).await?;
            let message = response.into_inner();

            let workload_ms = {
                let mut rng = rand::thread_rng();
                rng.gen_range(delay.0..=delay.1) as u64
            };
            tokio::time::sleep(Duration::from_millis(workload_ms)).await;

            if message.payload == STOP_TOKEN {
                println!("Received stop token");
                self.ack_method
                    .acknowledge(client, &message.uuid, AckMode::FORCE)
                    .await?;
                break;
            }

            self.ack_method
                .acknowledge(client, &message.uuid, AckMode::NORMAL)
                .await?;
        }
        Ok(())
    }
}

async fn run_consumer(
    args: Args,
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut consuming_method: Box<dyn ConsumingMethodStrategy> =
        match (&args.ack_method, &args.consuming_method) {
            (AckMethod::Auto, ConsumingMethod::Single) => Box::new(SingleConsumingMethod::new(
                args.source.clone(),
                Box::new(AutoAckMethod::default()),
            )),
            (AckMethod::Auto, ConsumingMethod::Stream) => Box::new(StreamConsumingMethod::new(
                args.source.clone(),
                Box::new(AutoAckMethod::default()),
            )),
            (AckMethod::Single, ConsumingMethod::Single) => Box::new(SingleConsumingMethod::new(
                args.source.clone(),
                Box::new(SingleAckMethod::default()),
            )),
            (AckMethod::Single, ConsumingMethod::Stream) => Box::new(StreamConsumingMethod::new(
                args.source.clone(),
                Box::new(SingleAckMethod::default()),
            )),
            (AckMethod::Bulk, ConsumingMethod::Single) => Box::new(SingleConsumingMethod::new(
                args.source.clone(),
                Box::new(BulkAckMethod::default()),
            )),
            (AckMethod::Bulk, ConsumingMethod::Stream) => Box::new(StreamConsumingMethod::new(
                args.source.clone(),
                Box::new(BulkAckMethod::default()),
            )),
        };

    consuming_method.consume(client, args).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut client = RuusterClient::connect(args.server_addr.clone())
        .await
        .expect("failed to create consumer client");

    run_consumer(args, &mut client).await?;

    Ok(())
}
