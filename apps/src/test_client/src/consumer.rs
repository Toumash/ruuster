use clap::{Parser, ValueEnum};
use protos::{ruuster_client::RuusterClient, ConsumeRequest};
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
    #[arg(short, long)]
    server_addr: String,

    #[arg(short, long)]
    source: String,

    #[arg(short, long)]
    consuming_method: ConsumingMethod,

    #[arg(short, long)]
    ack_method: AckMethod,

    #[arg(short, long, default_value_t = 0)]
    min_delay_ms: i32,

    #[arg(short, long, default_value_t = 0)]
    max_delay_ms: i32,
}

// 2 strategies - consume and ack, consume will call a chosen ack method based on a terminal parameter

type UuidSerialized = String;
const STOP_TOKEN: &str = "STOP";

#[async_trait]
trait AckMethodStrategy {
    async fn acknowledge(&self, uuid: &UuidSerialized) -> Result<(), Box<dyn std::error::Error>>;
    fn is_auto(&self) -> bool;
}

struct AutoAckMethod;

#[async_trait]
impl AckMethodStrategy for AutoAckMethod {
    async fn acknowledge(&self, _uuid: &UuidSerialized) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn is_auto(&self) -> bool {
        true
    }
}

#[async_trait]
trait ConsumingMethodStrategy<AckType: AckMethodStrategy + Send + Sync> {
    fn new(queue_name: String, ack: AckType) -> Self;
    async fn consume(
        &self,
        client: &mut RuusterClient<Channel>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

struct StramConsumingMethod<AckType: AckMethodStrategy + Send + Sync> {
    queue_name: String,
    ack_method: AckType
}

#[async_trait]
impl<AckType: AckMethodStrategy + Send + Sync + 'static> ConsumingMethodStrategy<AckType>
    for StramConsumingMethod<AckType>
{
    fn new(queue_name: String, ack_method: AckType) -> Self {
        StramConsumingMethod {
            queue_name,
            ack_method,
        }
    }
    async fn consume(
        &self,
        client: &mut RuusterClient<Channel>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = ConsumeRequest {
            queue_name: self.queue_name.clone(),
            auto_ack: self.ack_method.is_auto(),
        };

        let mut response_stream = client.consume_bulk(request).await?.into_inner();

        while let Some(message) = response_stream.message().await? {
            println!("Payload: {}", &message.payload);

            // simulate workload
            // let mut rng = rand::thread_rng();
            // let workload_sec = rng.gen_range(self.workload_range_sec.0 ..= self.workload_range_sec.1);
            // std::thread::sleep(Duration::from_millis((workload_sec * 1000) as u64));

            self.ack_method.acknowledge(&message.uuid).await?;

            if message.payload == STOP_TOKEN {
                println!("Received stop token");
                break;
            }
        }

        Ok(())
    }
}

async fn run_consumer(args: Args, client: &mut RuusterClient<Channel>) {
    let consuming_method = match (args.ack_method, args.consuming_method) {
        (AckMethod::Auto, ConsumingMethod::Single) => todo!(),
        (AckMethod::Auto, ConsumingMethod::Stream) => {
            StramConsumingMethod::new(args.source, AutoAckMethod)
        }
        (AckMethod::Single, ConsumingMethod::Single) => todo!(),
        (AckMethod::Single, ConsumingMethod::Stream) => todo!(),
        (AckMethod::Bulk, ConsumingMethod::Single) => todo!(),
        (AckMethod::Bulk, ConsumingMethod::Stream) => todo!(),
    };

    match consuming_method.consume(client).await {
        Ok(_) => println!("Success"),
        Err(e) => println!("Error: {:#?}", e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut client = RuusterClient::connect(args.server_addr.clone())
        .await
        .expect("failed to create consumer client");

    run_consumer(args, &mut client).await;

    Ok(())
}
