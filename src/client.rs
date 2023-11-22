use std::io;

use ruuster::{ruuster_client::RuusterClient, Empty, QueueDeclareRequest, ListenRequest};
use tonic::transport::Channel;

use crate::ruuster::{BindQueueToExchangeRequest, ExchangeDeclareRequest, ExchangeDefinition};

pub mod ruuster {
    tonic::include_proto!("ruuster");
}

fn console_input(msg: &str) -> Result<String, Box<dyn std::error::Error>> {
    println!("{}", msg);
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    Ok(buffer.trim().to_string())
}

fn handle_menu() -> i32 {
    println!("Ruuster gRPC queues demo");
    println!("Choose option: [0-3]");
    println!("[1] add queue");
    println!("[2] list queues");
    println!("[3] add exchange");
    println!("[4] list exchanges");
    println!("[5] bind queue to exchange");
    println!("[6] publish");
    println!("[7] start listening");
    println!("[0] quit");
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer).unwrap();

    let number = buffer.trim().parse();
    match number {
        Ok(n @ 0..=7) => return n,
        _ => {
            println!("Wrong option - exiting program");
            return 0;
        }
    }
}

async fn add_queue(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    println!("Enter queue name: ");
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    client
        .queue_declare(QueueDeclareRequest {
            queue_name: buffer.trim().into(),
        })
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
    println!("Enter exchange name: ");
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                exchange_name: buffer.trim().into(),
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
    println!("Type existring queue name: ");
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    let queue_name = buffer.trim();

    println!("Type existing exchange name: ");
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    let exchange_name = buffer.trim();

    client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            queue_name: queue_name.into(),
            exchange_name: exchange_name.into(),
        })
        .await?;

    Ok(())
}

async fn produce(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let exchange_name = console_input("Type exisintg exchange name: ")?;
    let payload = console_input("Type message to the other side: ")?;

    let message = ruuster::Message {
        header: Some(ruuster::MessageHeader {
            exchange_name: exchange_name,
        }),
        body: Some(ruuster::MessageBody {
            payload: payload,
        }),
    };

    client.produce(message).await?;

    Ok(())
}

async fn listen(client: &mut RuusterClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let queue_name = console_input("Type existing query name: ")?;
    let request = ListenRequest { queue_name };
    let mut response_stream = client.consume(request).await?.into_inner();
    while let Some(message) = response_stream.message().await? {
        println!("Received message: {:?}", message);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RuusterClient::connect("http://127.0.0.1:50051").await?;

    loop {
        let menu_opt = handle_menu();
        match menu_opt {
            1 => add_queue(&mut client).await?,
            2 => list_queues(&mut client).await?,
            3 => add_exchange(&mut client).await?,
            4 => list_exchanges(&mut client).await?,
            5 => bind_queue(&mut client).await?,
            6 => produce(&mut client).await?,
            7 => listen(&mut client).await?,
            0 => return Ok(()),
            _ => return Err("Runtime error".into()),
        }
        println!("-----------------------------");
    }
}
