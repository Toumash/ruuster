use crate::RpcOk;
use crate::server::RuusterServer;
use ruuster_protos::v1::topology_service_server::TopologyService;
use ruuster_protos::v1::{
    AddExchangeRequest, AddQueueRequest, BindRequest, RemoveExchangeRequest, RemoveQueueRequest,
    UnbindRequest,
};
use tonic::Status;
use crate::utils::{RpcRequest, RpcResponse};


#[tonic::async_trait]
impl TopologyService for RuusterServer {
    async fn add_queue(
        &self,
        request: RpcRequest<AddQueueRequest>,
    ) -> RpcResponse<()> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;
        let max_capacity = inner.max_capacity;

        self.topology_manager.add_queue(queue_name, max_capacity)
        .map_err(|e| {
            Status::internal(format!("Cannot add queue: {}", e))
        })?;

        RpcOk!()
    }

    async fn remove_queue(
        &self,
        request: RpcRequest<RemoveQueueRequest>,
    ) -> RpcResponse<()> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;

        self.topology_manager.remove_queue(&queue_name)
        .map_err(|e| {
            Status::internal(format!("Cannot remove queue: {}", e))
        })?;

        RpcOk!()
    }

    async fn add_exchange(
        &self,
        request: RpcRequest<AddExchangeRequest>,
    ) -> RpcResponse<()> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;

        // Use default DirectStrategy for now
        // TODO: Parse strategy from proto when we add strategy selection
        self.topology_manager.add_exchange(exchange_name, None).map_err(|e|{
            Status::internal(format!("Cannot add exchange: {}", e))
        })?;

        RpcOk!()
    }

    async fn remove_exchange(
        &self,
        request: RpcRequest<RemoveExchangeRequest>,
    ) -> RpcResponse<()> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;

        self.topology_manager.remove_exchange(&exchange_name)
        .map_err(|e| {
            Status::internal(format!("Cannot remove exchange: {}", e))
        })?;

        RpcOk!()
    }

    async fn bind(&self, request: RpcRequest<BindRequest>) -> RpcResponse<()> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;
        let queue_name = inner.queue_name;

        self.topology_manager.bind_queue(&exchange_name, &queue_name)
        .map_err(|e| {
            Status::internal(format!("Cannot bind queue: {}", e))
        })?;

        RpcOk!()
    }

    async fn unbind(&self, request: RpcRequest<UnbindRequest>) -> RpcResponse<()> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;
        let queue_name = inner.queue_name;

        self.topology_manager.unbind_queue(&exchange_name, &queue_name)
        .map_err(|e| {
            Status::internal(format!("Cannot unbind queue: {}", e))
        })?;

        RpcOk!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::RuusterServer;
    use ruuster_router::Router;
    use std::sync::Arc;

    fn setup_test_server() -> RuusterServer {
        let router = Arc::new(Router::new());
        RuusterServer::new(router)
    }

    #[tokio::test]
    async fn test_add_queue_success() {
        let server = setup_test_server();

        let request = tonic::Request::new(AddQueueRequest {
            queue_name: "test_queue".to_string(),
            max_capacity: 100,
            durable: false,
        });
        let response = server.add_queue(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_add_and_remove_queue() {
        let server = setup_test_server();

        // Add queue
        let add_req = tonic::Request::new(AddQueueRequest {
            queue_name: "test_queue".to_string(),
            max_capacity: 100,
            durable: false,
        });
        server.add_queue(add_req).await.unwrap();

        // Remove queue
        let remove_req = tonic::Request::new(RemoveQueueRequest {
            queue_name: "test_queue".to_string(),
        });
        let response = server.remove_queue(remove_req).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_queue() {
        let server = setup_test_server();

        let request = tonic::Request::new(RemoveQueueRequest {
            queue_name: "nonexistent".to_string(),
        });

        let response = server.remove_queue(request).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn test_add_exchange_success() {
        let server = setup_test_server();

        let request = tonic::Request::new(AddExchangeRequest {
            exchange_name: "test_exchange".to_string(),
            kind: 0, // Direct exchange
            durable: false,
        });
        let response = server.add_exchange(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_add_and_remove_exchange() {
        let server = setup_test_server();

        // Add exchange
        let add_req = tonic::Request::new(AddExchangeRequest {
            exchange_name: "test_exchange".to_string(),
            kind: 0, // Direct exchange
            durable: false,
        });
        server.add_exchange(add_req).await.unwrap();

        // Remove exchange
        let remove_req = tonic::Request::new(RemoveExchangeRequest {
            exchange_name: "test_exchange".to_string(),
        });
        let response = server.remove_exchange(remove_req).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_bind_queue_to_exchange() {
        let server = setup_test_server();

        // Setup: add queue and exchange
        server
            .add_queue(tonic::Request::new(AddQueueRequest {
                queue_name: "test_queue".to_string(),
                max_capacity: 100,
                durable: false,
            }))
            .await
            .unwrap();

        server
            .add_exchange(tonic::Request::new(AddExchangeRequest {
                exchange_name: "test_exchange".to_string(),
                kind: 0,
                durable: false,
            }))
            .await
            .unwrap();

        // Bind them
        let bind_req = tonic::Request::new(BindRequest {
            exchange_name: "test_exchange".to_string(),
            queue_name: "test_queue".to_string(),
            routing_key: None,
        });

        let response = server.bind(bind_req).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_bind_nonexistent_exchange() {
        let server = setup_test_server();

        server
            .add_queue(tonic::Request::new(AddQueueRequest {
                queue_name: "test_queue".to_string(),
                max_capacity: 100,
                durable: false,
            }))
            .await
            .unwrap();

        let bind_req = tonic::Request::new(BindRequest {
            exchange_name: "nonexistent_exchange".to_string(),
            queue_name: "test_queue".to_string(),
            routing_key: None,
        });

        let response = server.bind(bind_req).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("Cannot bind queue"));
    }

    #[tokio::test]
    async fn test_unbind_queue_from_exchange() {
        let server = setup_test_server();

        // Setup: add queue and exchange, then bind
        server
            .add_queue(tonic::Request::new(AddQueueRequest {
                queue_name: "test_queue".to_string(),
                max_capacity: 100,
                durable: false,
            }))
            .await
            .unwrap();

        server
            .add_exchange(tonic::Request::new(AddExchangeRequest {
                exchange_name: "test_exchange".to_string(),
                kind: 0,
                durable: false,
            }))
            .await
            .unwrap();

        server
            .bind(tonic::Request::new(BindRequest {
                exchange_name: "test_exchange".to_string(),
                queue_name: "test_queue".to_string(),
                routing_key: None,
            }))
            .await
            .unwrap();

        // Unbind them
        let unbind_req = tonic::Request::new(UnbindRequest {
            exchange_name: "test_exchange".to_string(),
            queue_name: "test_queue".to_string(),
            routing_key: None,
        });

        let response = server.unbind(unbind_req).await;
        assert!(response.is_ok());
    }
}
