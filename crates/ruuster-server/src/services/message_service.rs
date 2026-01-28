use futures::Stream;
use ruuster_internals::Message;
use ruuster_protos::v1::message_service_server::MessageService;
use ruuster_protos::v1::{ConsumeRequest, Message as ProtoMsg, ProduceRequest};
use std::pin::Pin;
use tonic::Status;

use crate::RpcOk;
use crate::server::RuusterServer;
use crate::utils::{RpcResponse, RpcRequest};

#[tonic::async_trait]
impl MessageService for RuusterServer {
    async fn produce(
        &self,
        request: RpcRequest<ProduceRequest>,
    ) -> RpcResponse<()> {
        let inner = request.into_inner();

        // 1. Validate & convert proto -> internal
        let proto_msg = inner
            .message
            .ok_or_else(|| Status::invalid_argument("Missing message"))?;
        let exchange_name = inner.exchange;

        let msg = Message::try_from(proto_msg)
            .map_err(|e| Status::internal(format!("Conversion error: {}", e)))?;

        // 2. Delegate to business logic layer
        self.message_handler
            .produce(&exchange_name, msg)
            .map_err(|e| Status::internal(format!("Produce error: {}", e)))?;
        RpcOk!()
    }

    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<ProtoMsg, Status>> + Send>>;

    async fn consume(
        &self,
        request: RpcRequest<ConsumeRequest>,
    ) -> RpcResponse<Self::ConsumeStream> {
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

        RpcOk!(Box::pin(proto_stream))
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
        let server = RuusterServer::new();

        server.router.add_exchange("test_ex", Box::new(DirectStrategy));
        let q = Arc::new(Queue::new("test_q".into(), 10));
        server.router.add_queue(Arc::clone(&q));

        if let Some(ex) = server.router.get_exchange("test_ex") {
            ex.bind(q);
        }

        server
    }

    #[tokio::test]
    async fn test_produce_success() {
        let server = setup_test_server().await;

        let req = RpcRequest::new(ProduceRequest {
            exchange: "test_ex".into(),
            message: Some(ProtoMsg {
                uuid: Uuid::new_v4().as_bytes().to_vec(),
                routing_key: Some("test_q".into()),
                payload: b"hello".to_vec(),
                ..Default::default()
            }),
        });

        let response = server.produce(req).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_produce_invalid_exchange() {
        let server = setup_test_server().await;
        let req = RpcRequest::new(ProduceRequest {
            exchange: "invalid_ex".into(),
            message: Some(ProtoMsg {
                uuid: Uuid::new_v4().as_bytes().to_vec(),
                ..Default::default()
            }),
        });

        let response = server.produce(req).await;
        assert!(response.is_err());
    }
}
