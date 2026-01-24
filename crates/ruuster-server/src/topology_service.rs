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

        self.router
            .add_queue(std::sync::Arc::new(ruuster_core::Queue::new(
                queue_name,
                max_capacity,
            )));

        ret_response!(AddQueueResponse)
    }

    async fn remove_queue(
        &self,
        request: RpcRequest<RemoveQueueRequest>,
    ) -> RpcResponse<RemoveQueueResponse> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;

        match self.router.remove_queue(&queue_name) {
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

        self.router
            .add_exchange(&exchange_name, Box::new(ruuster_router::DirectStrategy));

        ret_response!(AddExchangeResponse)
    }

    async fn remove_exchange(
        &self,
        request: RpcRequest<RemoveExchangeRequest>,
    ) -> RpcResponse<RemoveExchangeResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;

        match self.router.remove_exchange(&exchange_name) {
            Ok(_) => ret_response!(RemoveExchangeResponse),
            Err(e) => ret_response!(RemoveExchangeResponse, e.to_string()),
        }
    }

    async fn bind(&self, request: RpcRequest<BindRequest>) -> RpcResponse<BindResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;
        let queue_name = inner.queue_name;

        let exchange = match self.router.get_exchange(&exchange_name) {
            Some(ex) => ex,
            None => {
                return ret_response!(
                    BindResponse,
                    format!("Exchange '{}' not found", exchange_name)
                )
            }
        };

        let queue = match self.router.get_queue(&queue_name) {
            Some(q) => q,
            None => {
                return ret_response!(
                    BindResponse,
                    format!("Queue '{}' not found", queue_name)
                )
            }
        };

        exchange.bind(queue);

        ret_response!(BindResponse)
    }

    async fn unbind(&self, request: RpcRequest<UnbindRequest>) -> RpcResponse<UnbindResponse> {
        let inner = request.into_inner();
        let exchange_name = inner.exchange_name;
        let queue_name = inner.queue_name;

        let exchange = match self.router.get_exchange(&exchange_name) {
            Some(ex) => ex,
            None => {
                return ret_response!(
                    UnbindResponse,
                    format!("Exchange '{}' not found", exchange_name)
                )
            }
        };

        let queue = match self.router.get_queue(&queue_name) {
            Some(q) => q,
            None => {
                return ret_response!(
                    UnbindResponse,
                    format!("Queue '{}' not found", queue_name)
                )
            }
        };

        exchange.unbind(&queue.name);

        ret_response!(UnbindResponse)
    }
}
