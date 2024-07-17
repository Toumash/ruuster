use clap::{Parser, ValueEnum};
use protos::{ruuster_client::RuusterClient, AckMessageBulkRequest, AckRequest, ConsumeRequest};
use rand::Rng;
use std::cmp::PartialEq;
use std::time::Duration;
use tokio::time::Instant;
use tonic::{async_trait, transport::Channel, Status};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

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

// notice(msaff): this should probably be in some kind of config file global to all consumers
const BULK_ACK_AMOUNT: usize = 20;
const CONSUMER_TIMEOUT: Duration = Duration::from_secs(5);

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
        // empty uuids are sent when forcing cleanup with bulk-ack requests, it can be safely ignored here
        if uuid.is_empty() {
            return Ok(());
        }
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
            info!(amount=%self.uuids.len(), "acking messages with AckMode::FORCE");
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
            info!(amount=%self.uuids.len(), "acked bulk");
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
        info!(timeout=%CONSUMER_TIMEOUT.as_secs() ,"consuming...");
        let mut consume_counter = 0;
        while let Ok(response) =
            tokio::time::timeout(CONSUMER_TIMEOUT, response_stream.message()).await
        {
            let message = match response {
                Ok(msg_opt) => {
                    if let Some(msg) = msg_opt {
                        msg
                    } else {
                        error!("message is none");
                        return Err(Status::internal("message is none").into());
                    }
                }
                Err(e) => {
                    error!(error=%e, "stream consumer returned error");
                    return Err(e.into());
                }
            };
            // simulate workload
            let workload_ms = {
                let mut rng = rand::thread_rng();
                rng.gen_range(delay.0..=delay.1) as u64
            };
            tokio::time::sleep(Duration::from_millis(workload_ms)).await;
            debug!(uuid=%message.uuid, "consumed message");
            consume_counter += 1;

            self.ack_method
                .acknowledge(client, &message.uuid, AckMode::NORMAL)
                .await?;
        }

        info!(consume_counter=%consume_counter, "end of consuming");
        self.ack_method
            .acknowledge(client, &UuidSerialized::new(), AckMode::FORCE)
            .await?;
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
        info!(timeout=%CONSUMER_TIMEOUT.as_secs(), "consuming...");
        let mut consume_counter = 0;
        let mut start_time = Instant::now();
        loop {
            let iteration_time = Instant::now();
            if iteration_time - start_time > CONSUMER_TIMEOUT {
                break;
            }
            let request = ConsumeRequest {
                queue_name: self.queue_name.clone(),
                auto_ack: self.ack_method.is_auto(),
            };

            let message = match client.consume_one(request).await {
                Ok(response) => response.into_inner(),
                Err(e) => {
                    if e.code() == tonic::Code::NotFound {
                        debug!("no message on a queue, waiting for 100ms before trying again");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    } else {
                        error!(error=%e, "error occurred in single-consumer");
                        break;
                    }
                }
            };

            // TODO(msaff): there is a UniformDuration in rand crate
            let workload_ms = {
                let mut rng = rand::thread_rng();
                rng.gen_range(delay.0..=delay.1) as u64
            };
            tokio::time::sleep(Duration::from_millis(workload_ms)).await;
            debug!(uuid=%message.uuid, "consumed message");
            consume_counter += 1;

            self.ack_method
                .acknowledge(client, &message.uuid, AckMode::NORMAL)
                .await?;
            start_time = Instant::now();
        }
        info!(consume_counter=%consume_counter, "end of consuming");
        self.ack_method
            .acknowledge(client, &UuidSerialized::new(), AckMode::FORCE)
            .await?;
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
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let filter_layer = EnvFilter::try_from_default_env()?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter_layer)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    let mut client = RuusterClient::connect(args.server_addr.clone())
        .await
        .expect("failed to create consumer client");

    run_consumer(args, &mut client).await?;

    Ok(())
}
