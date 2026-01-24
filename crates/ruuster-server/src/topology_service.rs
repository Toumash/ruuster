use crate::server::RuusterServer;
use ruuster_protos::v1::topology_service_server::TopologyService;
use ruuster_protos::v1::{
    AddExchangeRequest, AddExchangeResponse, AddQueueRequest, AddQueueResponse, BindRequest,
    BindResponse, RemoveExchangeRequest, RemoveExchangeResponse, RemoveQueueRequest,
    RemoveQueueResponse, UnbindRequest, UnbindResponse,
};

type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
type RpcRequest<T> = tonic::Request<T>;

#[tonic::async_trait]
impl TopologyService for RuusterServer {
    async fn add_queue(
        &self,
        request: RpcRequest<AddQueueRequest>,
    ) -> RpcResponse<AddQueueResponse> {
        let inner = request.into_inner();
        let queue_name = inner.queue_name;
        let capacity = inner.max_capacity as usize;

        self.router
            .add_queue(std::sync::Arc::new(ruuster_core::Queue::new(
                queue_name, capacity,
            )));

        Ok(tonic::Response::new(AddQueueResponse {error_message: "".into(), success: true}))
    }

    async fn remove_queue(
        &self,
        request: RpcRequest<RemoveQueueRequest>,
    ) -> RpcResponse<RemoveQueueResponse> {
        todo!()
    }

    async fn add_exchange(
        &self,
        request: RpcRequest<AddExchangeRequest>,
    ) -> RpcResponse<AddExchangeResponse> {
        todo!()
    }

    async fn remove_exchange(
        &self,
        request: RpcRequest<RemoveExchangeRequest>,
    ) -> RpcResponse<RemoveExchangeResponse> {
        todo!()
    }

    async fn bind(&self, request: RpcRequest<BindRequest>) -> RpcResponse<BindResponse> {
        todo!()
    }

    async fn unbind(&self, request: RpcRequest<UnbindRequest>) -> RpcResponse<UnbindResponse> {
        todo!()
    }
}
