use crate::server::RuusterServer;
use ruuster_protos::v1::ack_service_server::AckService;
use ruuster_protos::v1::{
    AckBulkRequest, AckBulkResponse, AckRequest, AckResponse, NackBulkRequest, NackBulkResponse,
    NackRequest, NackResponse,
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
impl AckService for RuusterServer {
    async fn ack_message(&self, _request: RpcRequest<AckRequest>) -> RpcResponse<AckResponse> {
        // TODO: Implement actual ack logic
        ret_response!(AckResponse)
    }

    async fn ack_bulk(
        &self,
        _request: RpcRequest<AckBulkRequest>,
    ) -> RpcResponse<AckBulkResponse> {
        // TODO: Implement actual bulk ack logic
        ret_response!(AckBulkResponse)
    }

    async fn nack_message(
        &self,
        _request: RpcRequest<NackRequest>,
    ) -> RpcResponse<NackResponse> {
        // TODO: Implement actual nack logic
        ret_response!(NackResponse)
    }

    async fn nack_bulk(
        &self,
        _request: RpcRequest<NackBulkRequest>,
    ) -> RpcResponse<NackBulkResponse> {
        // TODO: Implement actual bulk nack logic
        ret_response!(NackBulkResponse)
    }
}
