use ruuster_protos::v1::message_service_client::MessageServiceClient;
use ruuster_protos::v1::{ConsumeRequest, Message as ProtoMsg, ProduceRequest};
use ruuster_protos::v1::topology_service_client::TopologyServiceClient;
use ruuster_protos::v1::{AddQueueRequest, AddExchangeRequest, BindRequest};
use ruuster_server::ServerConfig;
use std::time::Duration;
use tokio::time::timeout;
use tonic::Request;
use uuid::Uuid;

async fn create_default_topology(
    queue_name: &str,
    exchange_name: &str,
) {
    let mut client = TopologyServiceClient::connect("http://127.0.0.1:50051").await.unwrap();
    // Add Queue
    let add_queue_req = Request::new(AddQueueRequest {
        queue_name: queue_name.into(),
        max_capacity: 10,
        durable: false,
    });
    client.add_queue(add_queue_req).await.unwrap();

    // Add Exchange
    let add_exchange_req = Request::new(AddExchangeRequest {
        exchange_name: exchange_name.into(),
        kind: 0, // Direct exchange
        durable: false,
    });
    client.add_exchange(add_exchange_req).await.unwrap();

    // Bind Queue to Exchange
    let bind_req = Request::new(BindRequest {
        exchange_name: exchange_name.into(),
        queue_name: queue_name.into(),
        routing_key: None,
    });
    client.bind(bind_req).await.unwrap();
}

async fn setup_server_and_client() -> MessageServiceClient<tonic::transport::Channel> {
    // Spawn the server in the background
    let config = ServerConfig::default();
    tokio::spawn(async move {
        ruuster_server::run_server(config).await.unwrap();
    });

    // Give the server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect to the server
    MessageServiceClient::connect("http://127.0.0.1:50051")
        .await
        .expect("Failed to connect to server")
}

#[tokio::test]
async fn test_full_message_flow() {
    let mut client = setup_server_and_client().await;

    let queue_name = "default_q";
    let exchange_name = "default";

    // 1. Create a queue and bind it to the default exchange
    create_default_topology(queue_name, exchange_name).await;

    // 2. Start a Consumer Stream in a background task
    let consume_req = Request::new(ConsumeRequest {
        queue_name: queue_name.into(),
        prefetch_count: Some(10),
    });

    let mut stream = client.consume(consume_req).await.unwrap().into_inner();

    // 3. Produce a message
    let test_uuid = Uuid::new_v4();
    let produce_req = Request::new(ProduceRequest {
        exchange: exchange_name.into(),
        message: Some(ProtoMsg {
            uuid: test_uuid.as_bytes().to_vec(),
            routing_key: Some(queue_name.into()),
            payload: b"integration-test-payload".to_vec(),
            ..Default::default()
        }),
    });

    client.produce(produce_req).await.unwrap();

    // 4. Verify the message pops out of the stream
    // We use a timeout to avoid hanging forever if it fails
    match timeout(Duration::from_secs(2), stream.message()).await {
        Ok(Ok(Some(msg))) => {
            assert_eq!(msg.uuid, test_uuid.as_bytes().to_vec());
            println!("Successfully received message: {:?}", msg.uuid);
        }
        Ok(Ok(None)) => panic!("Stream ended without message"),
        Ok(Err(e)) => panic!("Stream error: {}", e),
        Err(_) => panic!("Timed out waiting for message from stream"),
    }
}
