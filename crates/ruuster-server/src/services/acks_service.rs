use crate::server::RuusterServer;
use ruuster_protos::v1::ack_service_server::AckService;
use ruuster_protos::v1::{
    AckBulkRequest, AckBulkResponse, AckRequest, AckResponse, NackBulkRequest, NackBulkResponse,
    NackRequest, NackResponse,
};
use uuid::Uuid;

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
    async fn ack_message(&self, request: RpcRequest<AckRequest>) -> RpcResponse<AckResponse> {
        let inner = request.into_inner();

        // Parse message UUID from string
        let message_id = match Uuid::parse_str(&inner.message_uuid) {
            Ok(id) => id,
            Err(e) => return ret_response!(AckResponse, format!("Invalid UUID: {}", e)),
        };

        // Apply acknowledgment
        match self.ack_manager.apply_ack(&message_id) {
            Ok(_) => {
                // TODO: Track consumer_id in message metadata to enable untracking
                // For now, we don't have consumer_id in AckRequest proto
                ret_response!(AckResponse)
            }
            Err(e) => ret_response!(AckResponse, format!("Ack failed: {:?}", e)),
        }
    }

    async fn ack_bulk(&self, request: RpcRequest<AckBulkRequest>) -> RpcResponse<AckBulkResponse> {
        let inner = request.into_inner();
        let mut failed_count = 0;
        let mut errors = Vec::new();

        for ack_req in &inner.ack_requests {
            let message_id = match Uuid::parse_str(&ack_req.message_uuid) {
                Ok(id) => id,
                Err(e) => {
                    failed_count += 1;
                    errors.push(format!("Invalid UUID: {}", e));
                    continue;
                }
            };

            if let Err(e) = self.ack_manager.apply_ack(&message_id) {
                failed_count += 1;
                errors.push(format!("{:?}", e));
            }
        }

        if failed_count == 0 {
            ret_response!(AckBulkResponse)
        } else {
            ret_response!(
                AckBulkResponse,
                format!("Failed to ack {} messages: {:?}", failed_count, errors)
            )
        }
    }

    async fn nack_message(&self, request: RpcRequest<NackRequest>) -> RpcResponse<NackResponse> {
        let inner = request.into_inner();

        // Parse message UUID from string
        let message_id = match Uuid::parse_str(&inner.message_uuid) {
            Ok(id) => id,
            Err(e) => return ret_response!(NackResponse, format!("Invalid UUID: {}", e)),
        };

        // Apply negative acknowledgment
        match self.ack_manager.apply_nack(&message_id) {
            Ok(_) => {
                // TODO: Implement requeue logic using inner.requeue flag
                // TODO: Get message from ack_manager and requeue to queue_name
                ret_response!(NackResponse)
            }
            Err(e) => ret_response!(NackResponse, format!("Nack failed: {:?}", e)),
        }
    }

    async fn nack_bulk(
        &self,
        request: RpcRequest<NackBulkRequest>,
    ) -> RpcResponse<NackBulkResponse> {
        let inner = request.into_inner();
        let mut failed_count = 0;
        let mut errors = Vec::new();

        for nack_req in &inner.nack_requests {
            let message_id = match Uuid::parse_str(&nack_req.message_uuid) {
                Ok(id) => id,
                Err(e) => {
                    failed_count += 1;
                    errors.push(format!("Invalid UUID: {}", e));
                    continue;
                }
            };

            if let Err(e) = self.ack_manager.apply_nack(&message_id) {
                failed_count += 1;
                errors.push(format!("{:?}", e));
            }
            // TODO: Implement requeue logic using nack_req.requeue flag
        }

        if failed_count == 0 {
            ret_response!(NackBulkResponse)
        } else {
            ret_response!(
                NackBulkResponse,
                format!("Failed to nack {} messages: {:?}", failed_count, errors)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::RuusterServer;
    use ruuster_internals::Message;
    use ruuster_router::Router;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn setup_test_server() -> RuusterServer {
        let router = Arc::new(Router::new());
        RuusterServer::new(router)
    }

    #[tokio::test]
    async fn test_ack_message_success() {
        let server = setup_test_server();

        // Setup: Add a message to ack manager
        let msg_id = Uuid::new_v4();
        let message = Message {
            uuid: msg_id,
            ..Default::default()
        };
        server
            .ack_manager
            .add_record(message, Duration::from_secs(60));

        let request = tonic::Request::new(AckRequest {
            queue_name: "test_queue".to_string(),
            message_uuid: msg_id.to_string(),
        });

        let response = server.ack_message(request).await.unwrap();
        assert!(response.into_inner().success);
    }

    #[tokio::test]
    async fn test_ack_message_invalid_uuid() {
        let server = setup_test_server();

        let request = tonic::Request::new(AckRequest {
            queue_name: "test_queue".to_string(),
            message_uuid: "invalid-uuid".to_string(),
        });

        let response = server.ack_message(request).await.unwrap();
        let inner = response.into_inner();
        assert!(!inner.success);
        assert!(inner.error_message.contains("Invalid UUID"));
    }

    #[tokio::test]
    async fn test_ack_message_not_found() {
        let server = setup_test_server();
        let msg_id = Uuid::new_v4();

        let request = tonic::Request::new(AckRequest {
            queue_name: "test_queue".to_string(),
            message_uuid: msg_id.to_string(),
        });

        let response = server.ack_message(request).await.unwrap();
        let inner = response.into_inner();
        assert!(!inner.success);
        assert!(inner.error_message.contains("Ack failed"));
    }

    #[tokio::test]
    async fn test_ack_bulk_success() {
        let server = setup_test_server();

        // Setup: Add messages to ack manager
        let msg_id1 = Uuid::new_v4();
        let msg_id2 = Uuid::new_v4();

        server.ack_manager.add_record(
            Message {
                uuid: msg_id1,
                ..Default::default()
            },
            Duration::from_secs(60),
        );
        server.ack_manager.add_record(
            Message {
                uuid: msg_id2,
                ..Default::default()
            },
            Duration::from_secs(60),
        );

        let request = tonic::Request::new(AckBulkRequest {
            ack_requests: vec![
                AckRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id1.to_string(),
                },
                AckRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id2.to_string(),
                },
            ],
        });

        let response = server.ack_bulk(request).await.unwrap();
        assert!(response.into_inner().success);
    }

    #[tokio::test]
    async fn test_ack_bulk_partial_failure() {
        let server = setup_test_server();

        // Setup: Add only one message
        let msg_id1 = Uuid::new_v4();
        let msg_id2 = Uuid::new_v4();

        server.ack_manager.add_record(
            Message {
                uuid: msg_id1,
                ..Default::default()
            },
            Duration::from_secs(60),
        );

        let request = tonic::Request::new(AckBulkRequest {
            ack_requests: vec![
                AckRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id1.to_string(),
                },
                AckRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id2.to_string(), // This one doesn't exist
                },
            ],
        });

        let response = server.ack_bulk(request).await.unwrap();
        let inner = response.into_inner();
        assert!(!inner.success);
        assert!(inner.error_message.contains("Failed to ack 1 messages"));
    }

    #[tokio::test]
    async fn test_nack_message_success() {
        let server = setup_test_server();

        // Setup: Add a message to ack manager
        let msg_id = Uuid::new_v4();
        let message = Message {
            uuid: msg_id,
            ..Default::default()
        };
        server
            .ack_manager
            .add_record(message, Duration::from_secs(60));

        let request = tonic::Request::new(NackRequest {
            queue_name: "test_queue".to_string(),
            message_uuid: msg_id.to_string(),
            requeue: true,
        });

        let response = server.nack_message(request).await.unwrap();
        assert!(response.into_inner().success);
    }

    #[tokio::test]
    async fn test_nack_bulk_success() {
        let server = setup_test_server();

        // Setup: Add messages to ack manager
        let msg_id1 = Uuid::new_v4();
        let msg_id2 = Uuid::new_v4();

        server.ack_manager.add_record(
            Message {
                uuid: msg_id1,
                ..Default::default()
            },
            Duration::from_secs(60),
        );
        server.ack_manager.add_record(
            Message {
                uuid: msg_id2,
                ..Default::default()
            },
            Duration::from_secs(60),
        );

        let request = tonic::Request::new(NackBulkRequest {
            nack_requests: vec![
                NackRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id1.to_string(),
                    requeue: true,
                },
                NackRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id2.to_string(),
                    requeue: false,
                },
            ],
        });

        let response = server.nack_bulk(request).await.unwrap();
        assert!(response.into_inner().success);
    }
}
