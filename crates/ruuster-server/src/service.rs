use futures::Stream;
use ruuster_internals::Message;
use ruuster_protos::v1::ruuster_service_server::RuusterService;
use ruuster_protos::v1::{ConsumeRequest, Message as ProtoMsg, ProduceRequest, ProduceResponse};
use ruuster_router::Router;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct RuusterServer {
    pub router: Arc<Router>,
}

impl RuusterServer {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }
}

#[tonic::async_trait]
impl RuusterService for RuusterServer {
    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let inner = request.into_inner();
        let proto_msg = inner
            .message
            .ok_or_else(|| Status::invalid_argument("Missing message"))?;
        let exchange_name = inner.exchange;

        let msg = Message::try_from(proto_msg)
            .map_err(|e| Status::internal(format!("Conversion error: {}", e)))?;

        match self.router.route(&exchange_name, msg) {
            Ok(_) => Ok(Response::new(ProduceResponse {
                success: true,
                error_message: "".into(),
            })),
            Err(e) => Ok(Response::new(ProduceResponse {
                success: false,
                error_message: e.to_string(),
            })),
        }
    }

    /// SERVER STREAMING: Client subscribes to a queue and receives a stream of messages
    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<ProtoMsg, Status>> + Send>>;

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;
        let router = Arc::clone(&self.router);

        let output = async_stream::try_stream! {
                while let Some(queue) = router.get_queue(&queue_name) {

                    if let Some(msg) = queue.dequeue()? {
                        yield ProtoMsg::from(msg);
                        continue;
                    }

                    queue.notify.notified().await;
                }

        };

        Ok(Response::new(Box::pin(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruuster_core::Queue;
    use ruuster_router::strategies::direct::DirectStrategy;
    use uuid::Uuid;

    async fn setup_test_server() -> RuusterServer {
        let router = Arc::new(Router::new());
        // Setup a default routing topology
        router.declare_exchange("test_ex", Box::new(DirectStrategy));
        let q = Arc::new(Queue::new("test_q".into(), 10));
        router.add_queue(Arc::clone(&q));

        if let Some(ex) = router.get_exchange("test_ex") {
            ex.bind(q);
        }

        RuusterServer::new(router)
    }

    #[tokio::test]
    async fn test_produce_success() {
        let server = setup_test_server().await;

        let req = Request::new(ProduceRequest {
            exchange: "test_ex".into(),
            message: Some(ProtoMsg {
                uuid: Uuid::new_v4().to_string(),
                routing_key: Some("test_q".into()),
                payload: b"hello".to_vec(),
                ..Default::default()
            }),
        });

        let response = server.produce(req).await.unwrap();
        assert!(response.into_inner().success);
    }

    #[tokio::test]
    async fn test_produce_invalid_exchange() {
        let server = setup_test_server().await;
        let req = Request::new(ProduceRequest {
            exchange: "invalid_ex".into(),
            message: Some(ProtoMsg {
                uuid: Uuid::new_v4().to_string(),
                ..Default::default()
            }),
        });

        let response = server.produce(req).await.unwrap();
        assert!(!response.into_inner().success);
    }
}
