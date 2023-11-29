use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;

use exchanges::types::*;
use exchanges::*;

use protos::ruuster;
use protos::*;

type QueueName = String;
type Queue = VecDeque<Message>;
type QueueContainer = HashMap<QueueName, Mutex<Queue>>;

pub struct RuusterQueues {
    queues: Arc<RwLock<QueueContainer>>,
    exchanges: Arc<RwLock<ExchangeContainer>>,
}

impl RuusterQueues {
    pub fn new() -> Self {
        RuusterQueues {
            queues: Arc::new(RwLock::new(QueueContainer::new())),
            exchanges: Arc::new(RwLock::new(ExchangeContainer::new())),
        }
    }
}

impl Default for RuusterQueues
{
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueues {
    async fn queue_declare(
        &self,
        request: tonic::Request<QueueDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        log::trace!("started queue_declare");
        let queue_name = request.get_ref().queue_name.clone();
        let mut queues_lock = self.queues.write().unwrap();
        if queues_lock.get(&queue_name).is_some() {
            let msg = format!("queue({}) already exists", queue_name.clone());
            log::error!("{}", msg);
            return Err(Status::already_exists(msg));
        }
        queues_lock.insert(
            queue_name,
            Mutex::new(VecDeque::new()),
        );
        log::trace!("queue declare finished successfully");
        Ok(Response::new(Empty {}))
    }

    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        log::trace!("started queue_declare");
        let mut exchanges_lock = self.exchanges.write().unwrap();
        let exchange_def = request.get_ref().exchange.clone().unwrap();
        match exchange_def.kind {
            0 => {
                if exchanges_lock.get(&exchange_def.exchange_name).is_some() {
                    let msg = format!("exchange({}) already exists", exchange_def.exchange_name.clone());
                    log::error!("{}", msg);
                    return Err(Status::already_exists(msg));
                }
                exchanges_lock.insert(
                    exchange_def.exchange_name,
                    Arc::new(RwLock::new(FanoutExchange::default())),
                );
            }
            other => {
                let msg = format!("exchange({}) not supported yet", other);
                log::error!("{}", msg);
                return Err(Status::unimplemented(msg));
            }
        };

        log::trace!("exchange_declare finished successfully");
        Ok(Response::new(Empty {}))
    }

    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        log::trace!("started list_queues");
        let queues_lock = self.queues.read().unwrap();
        let response = ListQueuesResponse {
            queue_names: queues_lock.iter().map(|queue| queue.0.clone()).collect(),
        };
        log::trace!("list_queues finished sucessfully");
        Ok(Response::new(response))
    }

    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        log::trace!("started list_exchanges");

        let exchanges_lock = self.exchanges.read().unwrap();
        let response = ListExchangesResponse {
            exchange_names: exchanges_lock
                .iter()
                .map(|exchange| exchange.0.clone())
                .collect(),
        };
        log::trace!("list_exchanges finished sucessfully");
        Ok(Response::new(response))
    }

    async fn bind_queue_to_exchange(
        &self,
        request: tonic::Request<BindQueueToExchangeRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        log::trace!("started bind_queue_to_exchange");

        let queue_name = &request.get_ref().queue_name;
        let exchange_name = &request.get_ref().exchange_name;

        let queues = self.queues.read().unwrap();
        let excahnges = self.exchanges.read().unwrap();

        let mut exchange_write = match (queues.get(queue_name), excahnges.get(exchange_name)) {
            (Some(_), Some(e)) => e.write().unwrap(),
            (_, _) => {
                let msg = "binding failed: requested queue or exchange doesn't exists";
                log::error!("{}", msg);
                return Err(Status::not_found(msg));
            }
        };

        match exchange_write.bind(queue_name) {
            Ok(()) => {
                log::trace!("bind_queue_to_exchange finished sucessfully");
                return Ok(Response::new(Empty {}));
            }
            Err(e) => {
                let msg = format!("binding failed: {}", e);
                log::error!("{}", msg);
                return Err(Status::internal(msg));
            }
        }
    }

    //NOTICE(msaff): this is not an only option, if it will not be good enough we can always change it to smoething more... lov-level ;)
    type ConsumeStream = ReceiverStream<Result<Message, Status>>;

    /**
     * this request will receive messages from specific queue and return it in form of asynchronous stream
     */
    async fn consume(
        &self,
        request: tonic::Request<ListenRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let queue_name = request.into_inner().queue_name;
        let (tx, rx) = mpsc::channel(4);
        let queues = self.queues.clone();

        tokio::spawn(async move {
            loop {
                let message = {
                    let queues_read = queues.read().unwrap();

                    let queue_arc = match queues_read.get(&queue_name) {
                        Some(queue) => queue,
                        None => {
                            eprintln!("requested queue doesn't exist");
                            return;
                        }
                    };

                    let mut queue = queue_arc.lock().unwrap();
                    queue.pop_front()
                };

                if let Some(message) = message {
                    if tx.send(Ok(message)).await.is_err() {
                        break;
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /**
     * for now let's have a simple producer that will push message into exchange requested in message header
     */
    async fn produce(&self, request: tonic::Request<Message>) -> Result<Response<Empty>, Status> {
        log::trace!("start produce");

        let msg = request.into_inner();

        let exchange_name = match &msg.header {
            Some(h) => h.exchange_name.clone(),
            None => return Err(Status::invalid_argument("message header is missing")),
        };

        let exchanges_read = self.exchanges.read().unwrap();

        let requested_exchange = match exchanges_read.get(&exchange_name) {
            Some(exchange) => exchange,
            None => return Err(Status::not_found("requested exchange doesn't exist")),
        };

        let requested_exchange_read = requested_exchange.read().unwrap();

        match requested_exchange_read.handle_message(&msg, self.queues.clone()) {
            Ok(()) => log::trace!("message sent"),
            Err(e) => log::error!("error occured while handling message: {}", e),
        };

        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests {

    use std::net::SocketAddr;
    use std::sync::Once;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use protos::ruuster::Empty;
    use protos::ruuster_client::RuusterClient;
    use protos::ruuster_server::RuusterServer;

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

        let response = client.queue_declare(QueueDeclareRequest {
            queue_name: "q1".to_string(),
        });
        assert!(response.await.is_ok(), "creating queue: q1 failed");

        let response = client.queue_declare(QueueDeclareRequest {
            queue_name: "q2".to_string(),
        });
        assert!(response.await.is_ok(), "creating queue: q2 failed");

        let response = client.queue_declare(QueueDeclareRequest {
            queue_name: "q3".to_string(),
        });
        assert!(response.await.is_ok(), "creating queue: q3 failed");

        // adding queue with duplicate name should fail
        let response = client.queue_declare(QueueDeclareRequest {
            queue_name: "q3".to_string(),
        });
        assert!(response.await.is_err(), "duplicate queue should fail");

        let list_response = client.list_queues(Empty {}).await;
        assert!(list_response.is_ok(), "listing queues failed");

        let list = list_response.unwrap();
        assert_eq!(list.get_ref().queue_names.len(), 3);
    }

    #[tokio::test]
    async fn test_declare_and_list_exchanges() {
        let addr = setup_server().await;
        let mut client = setup_client(addr).await.expect("failed to create client");

        let response = client.exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e1".to_string(),
            }),
        });
        assert!(response.await.is_ok(), "creating exchange: e1 failed");

        let response = client.exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e2".to_string(),
            }),
        });
        assert!(response.await.is_ok(), "creating exchange: e2 failed");

        let response = client.exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e3".to_string(),
            }),
        });
        assert!(response.await.is_ok(), "creating exchange: e3 failed");

        // adding exchange with duplicate name should fail
        let response = client.exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e3".to_string(),
            }),
        });
        assert!(response.await.is_err(), "duplicate exchange should fail");

        let list_response = client.list_exchanges(Empty {}).await;
        assert!(list_response.is_ok(), "failed to call list_exchanges");
        let list = list_response.unwrap();

        assert_eq!(list.get_ref().exchange_names.len(), 3);
    }

    #[tokio::test]
    async fn test_bind_queue() {
        let addr = setup_server().await;
        let mut client = setup_client(addr).await.expect("failed to create client");

        // add exchange
        let response = client.exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: "e1".to_string(),
            }),
        });
        assert!(response.await.is_ok(), "creating exchange failed");

        // add queue
        let response = client.queue_declare(QueueDeclareRequest {
            queue_name: "q1".to_string(),
        });
        assert!(response.await.is_ok(), "creating queue failed");

        let response = client.bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q1".to_string(),
        });
        assert!(response.await.is_ok(), "binding failed");

        let response = client.bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q1".to_string(),
        });
        assert!(response.await.is_err(), "creating binding failed");

        let response = client.bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e2".to_string(),
            queue_name: "q1".to_string(),
        });
        assert!(response.await.is_err(), "binding to non-existing exchange should fail");

        let response = client.bind_queue_to_exchange(BindQueueToExchangeRequest {
            exchange_name: "e1".to_string(),
            queue_name: "q2".to_string(),
        });
        assert!(response.await.is_err(), "binding non-existing queue should fail");
    }
}
