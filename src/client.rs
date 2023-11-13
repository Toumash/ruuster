use tonic::Request;

use mq::mq_client::MqClient;
use mq::ProduceRequest;
use std::io::{self};

pub mod mq {
    tonic::include_proto!("mq");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MqClient::connect("http://[::1]:50051").await?;

    loop {
        // std::thread::sleep(std::time::Duration::from_secs(1));
        println!("Yo, type Produce or Consume");
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer)?;

        match buffer.trim() {
            "Produce" => {
                println!("Type message to add to queue!");
                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                let request = Request::new(ProduceRequest {
                    message: input.to_owned(),
                });
                let _response = client.produce(request).await?;
            }
            "Consume" => {
                let request = Request::new(());
                let response = client.consume(request).await?;
                println!("RESPONSE={:?}", response);
            }
            _ => break,
        }
    }
    Ok(())
}
