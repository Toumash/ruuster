use std::net::SocketAddr;
use std::sync::Once;
use std::time::Duration;

use exchanges::{ExchangeKind, ExchangeName};
use protos::ExchangeDefinition;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

use protos::ruuster_client::RuusterClient;
use protos::ruuster_server::RuusterServer;

use queues::queues::QueueName;

use super::*;
use utils::generate_random_string;

const TEST_SERVER_ADDR: &str = "127.0.0.1:0";
const TEST_SERVER_DELAY: u64 = 100;

static ONCE: Once = Once::new();

async fn setup_server() -> SocketAddr {
    ONCE.call_once(|| {
        env_logger::init();
    });

    let listener = TcpListener::bind(TEST_SERVER_ADDR).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ruuster_queue_service = RuusterQueuesGrpc::new();

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

pub async fn setup_server_and_client() -> RuusterClient<Channel> {
    let addr = setup_server().await;
    setup_client(addr).await.expect("failed to create client")
}

pub async fn create_queues(
    client: &mut RuusterClient<Channel>,
    queue_names: &[QueueName],
    should_fail: bool,
) {
    for name in queue_names.iter() {
        let response = client
            .queue_declare(QueueDeclareRequest {
                queue_name: name.to_string(),
            })
            .await;

        if should_fail {
            assert!(response.is_err(), "creating queue {} should failed", name);
            return;
        }

        assert!(
            response.is_ok(),
            "creating queue {} failed: {}",
            name,
            response.unwrap_err()
        );
    }
}

pub async fn create_exchanges(
    client: &mut RuusterClient<Channel>,
    exchange_names: &[ExchangeName],
    exchange_kind: ExchangeKind,
    should_fail: bool,
) {
    let exchange_kind_id = exchange_kind as i32;
    for name in exchange_names.iter() {
        let response = client
            .exchange_declare(ExchangeDeclareRequest {
                exchange: Some(ExchangeDefinition {
                    kind: exchange_kind_id,
                    exchange_name: name.to_string(),
                }),
            })
            .await;

        if should_fail {
            assert!(
                response.is_err(),
                "creating exchange {} should failed",
                name
            );
            return;
        }

        assert!(
            response.is_ok(),
            "creating exchange: {} failed: {}",
            name,
            response.unwrap_err()
        );
    }
}

pub async fn create_bindings(
    client: &mut RuusterClient<Channel>,
    bindings: &[(QueueName, ExchangeName)],
    should_fail: bool,
) {
    for (queue_name, exchange_name) in bindings {
        let response = client
            .bind(BindRequest {
                exchange_name: exchange_name.to_string(),
                queue_name: queue_name.to_string(),
                metadata: None,
            })
            .await;

        if should_fail {
            assert!(response.is_err(), "creating binding should fail");
            return;
        }

        assert!(
            response.is_ok(),
            "binding {} to {} failed: {}",
            queue_name,
            exchange_name,
            response.unwrap_err()
        );
    }
}

pub async fn produce_n_random_messages(
    client: &mut RuusterClient<Channel>,
    exchange_name: ExchangeName,
    n: u32,
    should_fail: bool,
) -> Vec<String> {
    let mut result = vec![];
    for _ in 0..n {
        let payload = generate_random_string(1000);
        result.push(payload.clone());
        let request = ProduceRequest {
            exchange_name: exchange_name.clone(),
            payload,
            metadata: None,
        };

        let response = client.produce(request.clone()).await;

        if should_fail {
            assert!(response.is_err(), "producing message should fail");
        } else {
            assert!(
                response.is_ok(),
                "pushing message {:#?} to exchange {} failed: {}",
                request.payload,
                &exchange_name,
                response.unwrap_err()
            );
        }
    }
    result
}

pub async fn consume_messages(
    client: &mut RuusterClient<Channel>,
    queue_name: QueueName,
    payloads: &Vec<String>,
    should_fail: bool,
) {
    let request = ConsumeRequest {
        queue_name: queue_name.clone(),
        auto_ack: true,
    };
    for payload in payloads {
        let response = client.consume_one(request.clone()).await;
        if should_fail {
            assert!(response.is_err(), "consuming should fail");
        } else {
            assert!(
                response.is_ok(),
                "consuming from queue {} failed: {}",
                &queue_name,
                response.unwrap_err()
            );
            let consumed_payload = response.unwrap().into_inner().payload;
            assert_eq!(consumed_payload, payload.to_owned());
        }
    }
}

pub async fn consume_and_ack_messages(
    client: &mut RuusterClient<Channel>,
    queue_name: QueueName,
    should_ack_fail: bool,
    expected_message_count: u32,
) {
    let request = ConsumeRequest {
        queue_name: queue_name.clone(),
        auto_ack: false,
    };
    let mut idx = 0u32;
    loop {
        let response = client.consume_one(request.clone()).await;
        if response.is_err() {
            assert_eq!(response.unwrap_err().code(), tonic::Code::NotFound); // there is no message left in queue
            assert_eq!(idx, expected_message_count); // amount of messages is correct
            return;
        }
        let consumed_uuid = response.unwrap().into_inner().uuid;
        let ack_request = AckRequest {
            uuid: consumed_uuid.clone(),
        };
        let ack_response = client.ack_message(ack_request).await;
        if !should_ack_fail {
            assert!(
                ack_response.is_ok(),
                "ack request for message {} failed: {}",
                &consumed_uuid,
                ack_response.unwrap_err()
            );
        } else {
            assert!(ack_response.is_err(), "ack request should fail");
        }
        idx += 1;
    }
}

/**
 * single queue, single fanout exchange
 */
pub async fn setup_sqsfe_scenario(client: &mut RuusterClient<Channel>) {
    create_queues(client, &["q1".to_string()], false).await;
    create_exchanges(client, &["e1".to_string()], ExchangeKind::Fanout, false).await;
    create_bindings(client, &[("q1".to_string(), "e1".to_string())], false).await;
}

/**
 * multiple queues, single fanout exchange
 */
pub async fn setup_mqsfe_scenario(client: &mut RuusterClient<Channel>) {
    create_queues(client, &["q1".to_string(), "q2".to_string()], false).await;
    create_exchanges(client, &["e1".to_string()], ExchangeKind::Fanout, false).await;
    create_bindings(
        client,
        &[
            ("q1".to_string(), "e1".to_string()),
            ("q2".to_string(), "e1".to_string()),
        ],
        false,
    )
    .await;
}

/**
 * single queue, multiple fanout exchanges
 */
pub async fn setup_sqmfe_scenario(client: &mut RuusterClient<Channel>) {
    create_queues(client, &["q1".to_string()], false).await;
    create_exchanges(
        client,
        &["e1".to_string(), "e2".to_string()],
        ExchangeKind::Fanout,
        false,
    )
    .await;
    create_bindings(
        client,
        &[
            ("q1".to_string(), "e1".to_string()),
            ("q1".to_string(), "e2".to_string()),
        ],
        false,
    )
    .await;
}

/**
 * multiple queues, multiple fanout exchanges
 * bindings are cartesian product of both sets
 */
pub async fn setup_mqmfe_scenario(client: &mut RuusterClient<Channel>) {
    create_queues(client, &["q1".to_string(), "q2".to_string()], false).await;
    create_exchanges(
        client,
        &["e1".to_string(), "e2".to_string()],
        ExchangeKind::Fanout,
        false,
    )
    .await;
    create_bindings(
        client,
        &[
            ("q1".to_string(), "e1".to_string()),
            ("q1".to_string(), "e2".to_string()),
            ("q2".to_string(), "e1".to_string()),
            ("q2".to_string(), "e2".to_string()),
        ],
        false,
    )
    .await;
}
