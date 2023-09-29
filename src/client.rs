use hello_world::greeter_client::GreeterClient;
use hello_world::HelloRequest;
use std::io;

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://0.0.0.0:50051").await?;

    loop {
        let request = tonic::Request::new(HelloRequest {
            name: "Tonic".into(),
        });
        let response = client.say_hello(request).await?;
        println!("RESPONSE={:?}", response);
        // std::thread::sleep(std::time::Duration::from_secs(1));
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer)?;
    }
    Ok(())
}
