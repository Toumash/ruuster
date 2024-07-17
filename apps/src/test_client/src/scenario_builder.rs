use clap::Parser;
use config_definition::ScenarioConfig;
use protos::{ruuster_client::RuusterClient, BindRequest, ExchangeDeclareRequest, ExchangeDefinition, QueueDeclareRequest};
use tonic::transport::Channel;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod conf_parser;
mod config_definition;


struct ScenarioBuilder {
    config: ScenarioConfig,
    client: RuusterClient<Channel>,
}

// TODO(msaff): clap has a feature to generate files for autocompletion in bash and other terminals
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    server_addr: String,

    #[arg(long)]
    config_file: std::path::PathBuf,
}

impl ScenarioBuilder {
    async fn new(config: ScenarioConfig) -> Self {
        let client = RuusterClient::connect(config.server_metadata.server_addr.clone())
            .await
            .expect("failed to create ruuster client");
        ScenarioBuilder { config, client }
    }

    // TODO(msaff): those methods should be rewritten with actual builder pattern with state machine to prevent out of order calls, I'll do it in next iteration
    async fn setup_queues(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("setting up queues");
        for queue in &self.config.queues {
            self.client
                .queue_declare(QueueDeclareRequest {
                    queue_name: queue.name.to_owned(),
                })
                .await?;
            info!("added queue: {}", queue.name);
        }
        info!("queues are ready");
        Ok(())
    }

    async fn setup_exchanges(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("setting up exchanges");
        for exchange in &self.config.exchanges {
            self.client
                .exchange_declare(ExchangeDeclareRequest {
                    exchange: Some(ExchangeDefinition {
                        exchange_name: exchange.name.to_owned(),
                        kind: exchange.kind,
                    }),
                })
                .await?;
            info!(echange_name=%exchange.name ,"added exchange");
        }
        info!("exchanges are ready");
        Ok(())
    }

    async fn setup_binds(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("setting up binds");
        for exchange in &self.config.exchanges {
            let exchange_name = exchange.name.to_owned();
            for bind in &exchange.bindings {
                let bind_metadata = match &bind.bind_metadata {
                    None => None,
                    Some(bm) => Some(protos::BindMetadata {
                        routing_key: Some(protos::RoutingKey {
                            value: bm.routing_key.clone(),
                        }),
                    }),
                };
                self.client
                    .bind(BindRequest {
                        metadata: bind_metadata,
                        exchange_name: exchange_name.clone(),
                        queue_name: bind.queue_name.to_owned(),
                    })
                    .await?;
                info!(exchange_name=%exchange_name, queue_name=%bind.queue_name, "added bind");
            }
        }

        info!("bindings are ready");

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
    //TODO: export this to external module
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
    build_scenario(args).await?;

    Ok(())
}
