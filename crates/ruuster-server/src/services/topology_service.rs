use crate::server::RuusterServer;
use ruuster_protos::v1::topology_service_server::TopologyService;
use ruuster_protos::v1::{
    AddExchangeRequest, AddExchangeResponse, AddQueueRequest, AddQueueResponse, BindRequest,
    BindResponse, RemoveExchangeRequest, RemoveExchangeResponse, RemoveQueueRequest,
    RemoveQueueResponse, UnbindRequest, UnbindResponse,
};

type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type RpcRequest<T> = tonic::Request<T>;

macro_rules! ret_response {
    ($response_type:ident) => {{
        let response = $response_type {
            error_message: String::new(),
            success: true,
        };
        Ok(tonic::Response::new(response))
    }};
    ($response_type:ident, $error_msg:expr) => {{
        let response = $response_type {
            error_message: $error_msg.to_string(),
            success: false,
        };
        Ok(tonic::Response::new(response))
    }};
}

#[tonic::async_trait]
impl TopologyService for RuusterServer {
    async fn add_queue(
        &self,
        request: RpcRequest<AddQueueRequest>,
    ) -> RpcResponse<AddQueueResponse> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;
        let max_capacity = inner.max_capacity;

        match self.topology_manager.add_queue(queue_name, max_capacity) {
            Ok(_) => ret_response!(AddQueueResponse),
            Err(e) => ret_response!(AddQueueResponse, e.to_string()),
        }
    }

    async fn remove_queue(
        &self,
        request: RpcRequest<RemoveQueueRequest>,
    ) -> RpcResponse<RemoveQueueResponse> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;

        match self.topology_manager.remove_queue(&queue_name) {
            Ok(_) => ret_response!(RemoveQueueResponse),
            Err(e) => ret_response!(RemoveQueueResponse, e.to_string()),
        }
    }

    async fn add_exchange(
        &self,
        request: RpcRequest<AddExchangeRequest>,
    ) -> RpcResponse<AddExchangeResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;

        // Use default DirectStrategy for now
        // TODO: Parse strategy from proto when we add strategy selection
        match self.topology_manager.add_exchange(exchange_name, None) {
            Ok(_) => ret_response!(AddExchangeResponse),
            Err(e) => ret_response!(AddExchangeResponse, e.to_string()),
        }
    }

    async fn remove_exchange(
        &self,
        request: RpcRequest<RemoveExchangeRequest>,
    ) -> RpcResponse<RemoveExchangeResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;

        match self.topology_manager.remove_exchange(&exchange_name) {
            Ok(_) => ret_response!(RemoveExchangeResponse),
            Err(e) => ret_response!(RemoveExchangeResponse, e.to_string()),
        }
    }

    async fn bind(&self, request: RpcRequest<BindRequest>) -> RpcResponse<BindResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;
        let queue_name = inner.queue_name;

        match self
            .topology_manager
            .bind_queue(&exchange_name, &queue_name)
        {
            Ok(_) => ret_response!(BindResponse),
            Err(e) => ret_response!(BindResponse, e.to_string()),
        }
    }

    async fn unbind(&self, request: RpcRequest<UnbindRequest>) -> RpcResponse<UnbindResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;
        let queue_name = inner.queue_name;

        match self
            .topology_manager
            .unbind_queue(&exchange_name, &queue_name)
        {
            Ok(_) => ret_response!(UnbindResponse),
            Err(e) => ret_response!(UnbindResponse, e.to_string()),
        }
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
        let response = server.add_queue(request).await.unwrap();
        assert!(response.into_inner().success);
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
        let response = server.remove_queue(remove_req).await.unwrap();
        assert!(response.into_inner().success);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_queue() {
        let server = setup_test_server();

        let request = tonic::Request::new(RemoveQueueRequest {
            queue_name: "nonexistent".to_string(),
        });

        let response = server.remove_queue(request).await.unwrap();
        assert!(!response.into_inner().success);
    }

    #[tokio::test]
    async fn test_add_exchange_success() {
        let server = setup_test_server();

        let request = tonic::Request::new(AddExchangeRequest {
            exchange_name: "test_exchange".to_string(),
            kind: 0, // Direct exchange
            durable: false,
        });
        let response = server.add_exchange(request).await.unwrap();
        assert!(response.into_inner().success);
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
        let response = server.remove_exchange(remove_req).await.unwrap();
        assert!(response.into_inner().success);
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

        let response = server.bind(bind_req).await.unwrap();
        assert!(response.into_inner().success);
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

        let response = server.bind(bind_req).await.unwrap();
        let inner = response.into_inner();
        assert!(!inner.success);
        assert!(inner.error_message.contains("Exchange"));
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

        let response = server.unbind(unbind_req).await.unwrap();
        assert!(response.into_inner().success);
    }
}
