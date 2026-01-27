use futures::Stream;
use ruuster_internals::Message;
use ruuster_protos::v1::message_service_server::MessageService;
use ruuster_protos::v1::{ConsumeRequest, Message as ProtoMsg, ProduceRequest, ProduceResponse};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::server::RuusterServer;

#[tonic::async_trait]
impl MessageService for RuusterServer {
    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let inner = request.into_inner();

        // 1. Validate & convert proto -> internal
        let proto_msg = inner
            .message
            .ok_or_else(|| Status::invalid_argument("Missing message"))?;
        let exchange_name = inner.exchange;

        let msg = Message::try_from(proto_msg)
            .map_err(|e| Status::internal(format!("Conversion error: {}", e)))?;

        // 2. Delegate to business logic layer
        match self.message_handler.produce(&exchange_name, msg) {
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

    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<ProtoMsg, Status>> + Send>>;

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;

        // TODO: Get prefetch from request (extend proto)
        let prefetch_count = inner.prefetch_count.unwrap_or(1) as u16;

        // Delegate to message handler
        let msg_stream = self
            .message_handler
            .consume(&queue_name, prefetch_count)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Convert Message stream -> ProtoMsg stream
        let proto_stream = async_stream::try_stream! {
            for await msg in msg_stream {
                let msg = msg.map_err(|e| Status::internal(e.to_string()))?;
                yield ProtoMsg::from(msg);
            }
        };

        Ok(Response::new(Box::pin(proto_stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruuster_core::Queue;
    use ruuster_router::strategies::direct::DirectStrategy;
    use std::sync::Arc;
    use uuid::Uuid;

    async fn setup_test_server() -> RuusterServer {
        let router = Arc::new(ruuster_router::Router::new());
        // Setup a default routing topology
        router.add_exchange("test_ex", Box::new(DirectStrategy));
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
                uuid: Uuid::new_v4().as_bytes().to_vec(),
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
                uuid: Uuid::new_v4().as_bytes().to_vec(),
                ..Default::default()
            }),
        });

        let response = server.produce(req).await.unwrap();
        assert!(!response.into_inner().success);
    }
}
