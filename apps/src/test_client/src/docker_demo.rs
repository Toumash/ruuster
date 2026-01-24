use clap::Parser;
use protos::{
    ruuster_client::RuusterClient, BindRequest, ConsumeRequest, ExchangeDeclareRequest,
    ExchangeDefinition, ProduceRequest, QueueDeclareRequest,
};
use tonic::transport::Channel;
use tracing::{info, warn};

const DEFAULT_SERVER_ADDR: &str = "http://127.0.0.1:50051";
const DEFAULT_EXCHANGE_NAME: &str = "demo_exchange";
const DEFAULT_QUEUE_NAME: &str = "demo_queue";
const DEFAULT_MESSAGE_COUNT: u32 = 5;
const FANOUT_KIND: i32 = 0;

#[derive(Parser, Debug)]
#[command(version, about = "Ruuster docker demo client", long_about = None)]
struct Args {
    #[arg(long, default_value = DEFAULT_SERVER_ADDR)]
    server_addr: String,
    #[arg(long, default_value = DEFAULT_EXCHANGE_NAME)]
    exchange_name: String,
    #[arg(long, default_value = DEFAULT_QUEUE_NAME)]
    queue_name: String,
    #[arg(long, default_value_t = DEFAULT_MESSAGE_COUNT)]
    message_count: u32,
}

async fn declare_queue(
    client: &mut RuusterClient<Channel>,
    queue_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = client
        .queue_declare(QueueDeclareRequest {
            queue_name: queue_name.to_string(),
        })
        .await
    {
        warn!(queue_name=%queue_name, error=%err, "queue declare failed");
    }
    Ok(())
}

async fn add_exchange(
    client: &mut RuusterClient<Channel>,
    exchange_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: FANOUT_KIND,
                exchange_name: exchange_name.to_string(),
            }),
        })
        .await
    {
        warn!(exchange_name=%exchange_name, error=%err, "exchange declare failed");
    }
    Ok(())
}

async fn bind_queue(
    client: &mut RuusterClient<Channel>,
    exchange_name: &str,
    queue_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = client
        .bind(BindRequest {
            exchange_name: exchange_name.to_string(),
            queue_name: queue_name.to_string(),
            metadata: None,
        })
        .await
    {
        warn!(
            exchange_name=%exchange_name,
            queue_name=%queue_name,
            error=%err,
            "bind failed"
        );
    }
    Ok(())
}

async fn produce_messages(
    client: &mut RuusterClient<Channel>,
    exchange_name: &str,
    message_count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    for index in 0..message_count {
        let payload = format!("demo-message-{}", index + 1);
        client
            .produce(ProduceRequest {
                exchange_name: exchange_name.to_string(),
                payload: payload.clone(),
                metadata: None,
            })
            .await?;
        info!(payload=%payload, "produced message");
    }
    Ok(())
}

async fn consume_messages(
    client: &mut RuusterClient<Channel>,
    queue_name: &str,
    message_count: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = ConsumeRequest {
        queue_name: queue_name.to_string(),
        auto_ack: true,
    };
    for _ in 0..message_count {
        let response = client.consume_one(request.clone()).await?;
        let payload = response.into_inner().payload;
        info!(payload=%payload, "consumed message");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to setup docker demo logs");

    let args = Args::parse();
    let mut client = RuusterClient::connect(args.server_addr.clone())
        .await
        .expect("failed to create ruuster client");

    declare_queue(&mut client, &args.queue_name).await?;
    add_exchange(&mut client, &args.exchange_name).await?;
    bind_queue(&mut client, &args.exchange_name, &args.queue_name).await?;

    info!(
        exchange_name=%args.exchange_name,
        queue_name=%args.queue_name,
        message_count=%args.message_count,
        "running docker demo"
    );

    produce_messages(&mut client, &args.exchange_name, args.message_count).await?;
    consume_messages(&mut client, &args.queue_name, args.message_count).await?;

    info!("docker demo finished");
    Ok(())
}
