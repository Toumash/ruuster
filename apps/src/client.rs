use std::io;

use tonic::transport::Channel;
use uuid::Uuid;

use ruuster::{ruuster_client::RuusterClient, Empty, ConsumeRequest, QueueDeclareRequest};
use ruuster::{BindQueueToExchangeRequest, ExchangeDeclareRequest, ExchangeDefinition};

use protos::ruuster;
use utils::console_input;

fn handle_menu() -> i32 {
    println!("Ruuster gRPC queues demo");
    println!("Choose option: [0-8]");
    println!("[1] add queue");
    println!("[2] list queues");
    println!("[3] add exchange");
    println!("[4] list exchanges");
    println!("[5] bind queue to exchange");
    println!("[6] publish");
    println!("[7] start consuming");
    println!("[8] consume one message");
    println!("[9] consume one message (no ack)");
    println!("[0] quit");
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer).unwrap();

    let number = buffer.trim().parse();
    match number {
        Ok(n @ 0..=8) => n,
        _ => {
            println!("Wrong option - exiting program");
            0
        }
    }
}

async fn add_queue(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let queue_name = console_input("Type queue name")?;
    client
        .queue_declare(QueueDeclareRequest { queue_name })
        .await?;

    Ok(())
}

async fn list_queues(
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("List of queues:");
    let response = client.list_queues(Empty {}).await?;
    for entry in response.get_ref().queue_names.iter() {
        println!("{}", entry);
    }
    Ok(())
}

async fn add_exchange(
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let exchange_name = console_input("Type exchange name: ")?;
    client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                exchange_name,
                kind: 0,
            }),
        })
        .await?;

    Ok(())
}

async fn list_exchanges(
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("List of exchanges:");
    let response = client.list_exchanges(Empty {}).await?;
    for entry in response.get_ref().exchange_names.iter() {
        println!("{}", entry);
    }
    Ok(())
}

async fn bind_queue(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let queue_name = console_input("Type queue name: ")?;
    let exchange_name = console_input("Type exchange name: ")?;

    client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            queue_name,
            exchange_name,
        })
        .await?;

    Ok(())
}

async fn produce(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let exchange_name = console_input("Type exisintg exchange name: ")?;
    let payload = console_input("Type message to the other side: ")?;

    let amount_str = console_input("Type number of repeats")?;
    let amount = match amount_str.parse::<u32>() {
        Ok(n) => n,
        Err(e) => {
            return Err(format!("Failed to parse: {}", e).into());
        }
    };

    for _ in 0..amount {
        let message = ruuster::Message {
            uuid: Uuid::new_v4().to_string(),
            payload: payload.clone(),
        };
        let request = ruuster::ProduceRequest {
            payload: Some(message),
            exchange_name: exchange_name.clone(),
        };
        client.produce(request).await?;
    }

    Ok(())
}

async fn listen(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let queue_name = console_input("Type existing queue name: ")?;
    let request = ConsumeRequest { queue_name, auto_ack: true };
    let mut response_stream = client.consume(request).await?.into_inner();
    while let Some(message) = response_stream.message().await? {
        println!("Received message: {:#?}", message);
    }

    Ok(())
}

async fn consume_one_message(client: &mut RuusterClient<Channel>, auto_ack: bool) -> Result<(), Box<dyn std::error::Error>> {
    let queue_name = console_input("Type existing queue name: ")?;
    let request = ConsumeRequest{ queue_name, auto_ack: auto_ack };
    let response = client.consume_one(request).await?;
    println!("Received message: {:#?}", response);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RuusterClient::connect("http://127.0.0.1:50051").await?;

    loop {
        let menu_opt = handle_menu();
        let error = match menu_opt {
            1 => add_queue(&mut client).await,
            2 => list_queues(&mut client).await,
            3 => add_exchange(&mut client).await,
            4 => list_exchanges(&mut client).await,
            5 => bind_queue(&mut client).await,
            6 => produce(&mut client).await,
            7 => listen(&mut client).await,
            8 => consume_one_message(&mut client, true).await,
            9 => consume_one_message(&mut client, false).await,
            0 => return Ok(()),
            _ => return Err("wrong menu option".into()),
        };
        if let Err(e) = error {
            println!("Non critical error occured: {:#?}", e);
        }
        println!("-----------------------------");
    }
}
