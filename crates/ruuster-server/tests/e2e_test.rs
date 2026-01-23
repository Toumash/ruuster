use ruuster_protos::v1::ruuster_service_client::RuusterServiceClient;
use ruuster_protos::v1::{ConsumeRequest, Message as ProtoMsg, ProduceRequest};
use std::time::Duration;
use tokio::time::timeout;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn test_full_message_flow() {
    // 1. We assume the server is running on 50051
    // (In a real test, you'd spawn the server task here)
    let mut client = RuusterServiceClient::connect("http://127.0.0.1:50051")
        .await
        .expect("Failed to connect to server. Is it running?");

    let queue_name = "default_q";
    let exchange_name = "default";

    // 2. Start a Consumer Stream in a background task
    let consume_req = Request::new(ConsumeRequest {
        queue_name: queue_name.into(),
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
