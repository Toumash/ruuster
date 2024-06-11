use std::{env, path::PathBuf, str::FromStr, sync::Arc};

use crate::conf_json_def::ScenarioConfig;
use protos::{
    ruuster_client::RuusterClient, BindRequest, ConsumeRequest,
    ExchangeDeclareRequest, ExchangeDefinition, ProduceRequest, QueueDeclareRequest,
};
use tokio::{io::Join, sync::Notify, task::JoinHandle};
use tonic::transport::Channel;

mod conf_parser;
mod conf_json_def;

struct ScenarioRunner {
    config: ScenarioConfig,
    orchestrator: RuusterClient<Channel>,
    start_producing: Arc<Notify>,
    start_consuming: Arc<Notify>,
}

impl ScenarioRunner {
    async fn new(config: ScenarioConfig) -> Self {
        let orchestrator = RuusterClient::connect(config.metadata.server_addr.clone())
            .await
            .expect("failed to create orchestrator client");
        ScenarioRunner {
            config,
            orchestrator,
            start_producing: Arc::new(Notify::new()),
            start_consuming: Arc::new(Notify::new()),
        }
    }

    async fn setup_queues(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("setting up queues");
        for queue in &self.config.queues {
            self.orchestrator
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
            self.orchestrator
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
                self.orchestrator
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

    async fn setup_producers(&mut self) -> Result<JoinHandle<()>, Box<dyn std::error::Error>> {
        let producers = self.config.producers.clone();
        let server_addr = self.config.metadata.server_addr.clone();

        let handle = tokio::spawn(
            async move {
                let mut producer_client = RuusterClient::connect(server_addr)
                    .await
                    .expect("failed to create producer client");
                
            }
        );
        // for producer in &self.config.producers {
        //     let message_count = producer.messages_produced as i64;
        //     let start_producing = self.start_producing.clone();
        //     let server_addr = self.config.metadata.server_addr.clone();
        //     let payload_size = producer.message_payload_bytes;
        //     let exchange_name = producer.destination.clone();
        //     let producer_name = producer.name.clone();
        //     println!("creating producer client: {:#?}", producer);
        //     result.push(tokio::spawn(async move {
        //         let mut producer_client = RuusterClient::connect(server_addr)
        //             .await
        //             .expect("failed to create producer client");
        //         println!("producer {} created", &producer_name);
        //         start_producing.notified().await;
        //         println!("started producing");
        //         let i = 0;
        //         while i < message_count {
        //             let payload = utils::generate_random_string(payload_size as usize);
        //             let request = ProduceRequest {
        //                 metadata: None,
        //                 exchange_name: exchange_name.clone(),
        //                 payload,
        //             };
        //             producer_client
        //                 .produce(request)
        //                 .await
        //                 .expect("failed to produce mesage");
        //         }
        //         let stop_request = ProduceRequest {
        //             metadata: None,
        //             exchange_name: exchange_name.clone(),
        //             payload: "stop".to_string(),
        //         };
        //         producer_client
        //                 .produce(stop_request)
        //                 .await
        //                 .expect("failed to produce stop message");
        //         println!("{} messages sent", message_count);
        //     }));
        // }
        Ok(handle)
    }

    //TODO: add implementation for ack methods, and consuming methods
    // async fn setup_consumers(&mut self) -> Result<Vec<JoinHandle<()>>, Box<dyn std::error::Error>> {
    //     let mut result: Vec<JoinHandle<()>> = Vec::new();
    //     for consumer in &self.config.consumers {
    //         let start_consuming = self.start_consuming.clone();
    //         let server_addr = self.config.metadata.server_addr.clone();
    //         let source = consumer.source.clone();
    //         let auto_ack = match consumer.ack_method.as_str() {
    //             "auto" => true,
    //             _ => false,
    //         };
    //         let consumer_name = consumer.name.clone();
    //         let workload_range = consumer.workload_range_seconds.clone();
    //         println!("creating consumer: {:#?}", consumer);
    //         result.push(tokio::spawn(async move {
    //             let mut consumer_client = RuusterClient::connect(server_addr)
    //                 .await
    //                 .expect("failed to create consumer client");
    //             println!("consumer {} created", consumer_name);
    //             start_consuming.notified().await;
    //             println!("consuming started");
    //             let request = ConsumeRequest {
    //                 queue_name: source,
    //                 auto_ack,
    //             };
    //             let mut response_stream = consumer_client
    //                 .consume_bulk(request)
    //                 .await
    //                 .expect("failed to send bulk consuming request")
    //                 .into_inner();
    //             while let Some(message) = response_stream
    //                 .message()
    //                 .await
    //                 .expect("failed to recive message from channel")
    //             {
    //                 let workload = (utils::random_float(workload_range.min, workload_range.max) * 1_000.0) as u64;
    //                 // tokio::time::sleep(tokio::time::Duration::from_millis(workload)).await;
    //                 if message.payload == "stop" {
    //                     println!("received stop message");
    //                     break;
    //                 }
    //             }
    //         }));
    //     }
    //     Ok(result)
    // }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let path_buf = PathBuf::from_str(&args[1])?;
    let config = conf_parser::get_conf(&path_buf)?;

    let mut runner = ScenarioRunner::new(config).await;
    runner.setup_queues().await?;
    runner.setup_exchanges().await?;
    runner.setup_binds().await?;
    let producers_handles = runner.setup_producers().await?;


    runner.start_producing.notify_waiters();
    
    Ok(())
}
