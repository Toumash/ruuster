use std::net::SocketAddr;
use std::sync::Once;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use protos::ruuster::Empty;
use protos::ruuster_client::RuusterClient;
use protos::ruuster_server::RuusterServer;
use uuid::Uuid;

use super::*;

const TEST_SERVER_ADDR: &str = "127.0.0.1:0";
const TEST_SERVER_DELAY: u64 = 100;

static ONCE: Once = Once::new();

async fn setup_server() -> SocketAddr {
    ONCE.call_once(|| {
        env_logger::init();
    });

    println!("seting up a server");
    let listener = TcpListener::bind(TEST_SERVER_ADDR).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ruuster_queue_service = RuusterQueues::new();

    tokio::spawn(async move {
        Server::builder()
            .add_service(RuusterServer::new(ruuster_queue_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(TEST_SERVER_DELAY)).await;
    addr
}

async fn setup_client(
    addr: SocketAddr,
) -> Result<RuusterClient<tonic::transport::Channel>, tonic::transport::Error> {
    RuusterClient::connect(format!("http://{}", addr)).await
}

#[tokio::test]
async fn test_declare_and_list_queues() {
    let addr = setup_server().await;
    let mut client = setup_client(addr).await.expect("failed to create client");

    let response = client
        .queue_declare(QueueDeclareRequest {
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating queue q1 failed: {}",
        response.unwrap_err()
    );

    let response = client
        .queue_declare(QueueDeclareRequest {
            queue_name: "q2".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating queue: q2 failed: {}",
        response.unwrap_err()
    );

    let response = client
        .queue_declare(QueueDeclareRequest {
            queue_name: "q3".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating queue: q3 failed: {}",
        response.unwrap_err()
    );

    // adding queue with duplicate name should fail
    let response = client
        .queue_declare(QueueDeclareRequest {
            queue_name: "q3".to_string(),
        })
        .await;
    assert!(response.is_err(), "duplicate queue should fail");

    let list_response = client.list_queues(Empty {}).await;
    assert!(list_response.is_ok(), "listing queues failed");

    let list = list_response.unwrap();
    assert_eq!(list.get_ref().queue_names.len(), 3);
}

#[tokio::test]
async fn test_declare_and_list_exchanges() {
    let addr = setup_server().await;
    let mut client = setup_client(addr).await.expect("failed to create client");

    let response = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e1".to_string(),
            }),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating exchange: e1 failed: {}",
        response.unwrap_err()
    );

    let response = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e2".to_string(),
            }),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating exchange: e2 failed: {}",
        response.unwrap_err()
    );

    let response = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e3".to_string(),
            }),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating exchange: e3 failed: {}",
        response.unwrap_err()
    );

    // adding exchange with duplicate name should fail
    let response = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e3".to_string(),
            }),
        })
        .await;
    assert!(response.is_err(), "duplicate exchange should fail");

    let response = client.list_exchanges(Empty {}).await;
    assert!(
        response.is_ok(),
        "failed to call list_exchanges{}",
        response.unwrap_err()
    );
    let list = response.unwrap();

    assert_eq!(list.get_ref().exchange_names.len(), 3);
}

#[tokio::test]
async fn test_bind_queue() {
    let addr = setup_server().await;
    let mut client = setup_client(addr).await.expect("failed to create client");

    // add exchange
    let response = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e1".to_string(),
            }),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating exchange failed: {}",
        response.unwrap_err()
    );

    // add queue
    let response = client
        .queue_declare(QueueDeclareRequest {
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating queue failed: {}",
        response.unwrap_err()
    );

    let response = client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "binding failed: {}",
        response.unwrap_err()
    );

    let response = client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_err(),
        "creating binding failed: {}",
        response.unwrap_err()
    );

    let response = client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e2".to_string(),
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_err(),
        "binding to non-existing exchange should fail"
    );

    let response = client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q2".to_string(),
        })
        .await;
    assert!(response.is_err(), "binding non-existing queue should fail");
}

#[tokio::test]
async fn test_produce_and_consume() {
    let addr = setup_server().await;
    let mut client = setup_client(addr).await.expect("failed to create client");

    let response = client
        .queue_declare(QueueDeclareRequest {
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating queue: q1 failed: {}",
        response.unwrap_err()
    );

    let response = client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e1".to_string(),
            }),
        })
        .await;
    assert!(
        response.is_ok(),
        "creating exchange failed: {}",
        response.unwrap_err()
    );

    let response = client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q1".to_string(),
        })
        .await;
    assert!(
        response.is_ok(),
        "binding failed: {}",
        response.unwrap_err()
    );

    let uuid = Uuid::new_v4().to_string();
    let payload = "#abadcaffe".to_string();

    let response = client
        .produce(ProduceRequest {
            exchange_name: "e1".to_string(),
            payload: Some(Message { uuid, payload }),
        })
        .await;
    assert!(
        response.is_ok(),
        "producing message failed: {}",
        response.unwrap_err()
    );

    let response = client
        .consume_one(ConsumeRequest {
            queue_name: "q1".to_string(),
            auto_ack: true,
        })
        .await;

    assert!(
        response.is_ok(),
        "consuming sent message failed: {}",
        response.unwrap_err()
    );
}
