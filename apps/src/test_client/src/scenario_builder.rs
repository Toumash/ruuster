use clap::Parser;
use conf_json_def::ScenarioConfig;
use protos::{
    ruuster_client::RuusterClient, BindRequest, ExchangeDeclareRequest, ExchangeDefinition,
    QueueDeclareRequest,
};
use tonic::transport::Channel;

mod conf_json_def;
mod conf_parser;

struct ScenarioBuilder {
    config: ScenarioConfig,
    client: RuusterClient<Channel>,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    server_addr: String,

    #[arg(short, long)]
    config_file: std::path::PathBuf,
}

impl ScenarioBuilder {
    async fn new(config: ScenarioConfig) -> Self {
        let client = RuusterClient::connect(config.metadata.server_addr.clone())
            .await
            .expect("failed to create ruuster client");
        ScenarioBuilder { config, client }
    }

    async fn setup_queues(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("setting up queues");
        for queue in &self.config.queues {
            self.client
                .queue_declare(QueueDeclareRequest {
                    queue_name: queue.name.to_owned(),
                })
                .await?;
            println!("added queue: {}", queue.name);
        }
        println!("queues ready");
        Ok(())
    }

    async fn setup_exchanges(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("setting up exchanges");
        for exchange in &self.config.exchanges {
            self.client
                .exchange_declare(ExchangeDeclareRequest {
                    exchange: Some(ExchangeDefinition {
                        exchange_name: exchange.name.to_owned(),
                        kind: exchange.kind,
                    }),
                })
                .await?;
            println!("added exchange: {:?}", exchange);
        }
        println!("exchanges ready");
        Ok(())
    }

    async fn setup_binds(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("setting up binds");
        for exchange in &self.config.exchanges {
            let exchange_name = exchange.name.to_owned();
            for bind in &exchange.bindings {
                self.client
                    .bind(BindRequest {
                        metadata: None,
                        exchange_name: exchange_name.clone(),
                        queue_name: bind.queue_name.to_owned(),
                    })
                    .await?;
            }
            println!("added bindings for exchange: {}", &exchange_name);
        }

        Ok(())
    }
}

async fn build_scenario(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let config = conf_parser::get_conf(&args.config_file)?;
    let mut builder = ScenarioBuilder::new(config).await;
    builder.setup_queues().await?;
    builder.setup_exchanges().await?;
    builder.setup_binds().await?;
    return Ok(());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    build_scenario(args).await?;

    Ok(())
}
