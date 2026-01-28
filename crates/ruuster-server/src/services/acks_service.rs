use crate::RpcOk;
use crate::server::RuusterServer;
use crate::utils::{RpcRequest, RpcResponse, parse_uuid};
use ruuster_protos::v1::ack_service_server::AckService;
use ruuster_protos::v1::{AckBulkRequest, AckRequest, NackBulkRequest, NackRequest};
use tonic::Status;

#[tonic::async_trait]
impl AckService for RuusterServer {
    async fn ack_message(&self, request: RpcRequest<AckRequest>) -> RpcResponse<()> {
        let inner = request.into_inner();

        let message_id = parse_uuid(inner.message_uuid.as_slice())?;

        self.ack_manager
            .apply_ack(&message_id)
            .map_err(|e| Status::internal(format!("Ack failed: {:?}", e)))?;

        RpcOk!()
    }

    async fn ack_bulk(&self, request: RpcRequest<AckBulkRequest>) -> RpcResponse<()> {
        let inner = request.into_inner();
        let mut failed_count = 0;
        let mut errors = Vec::new();

        for ack_req in &inner.ack_requests {
            let message_id = parse_uuid(&ack_req.message_uuid.as_slice())?;

            if let Err(e) = self.ack_manager.apply_ack(&message_id) {
                failed_count += 1;
                errors.push(format!("{:?}", e));
            }
        }

        if failed_count > 0 {
            return Err(Status::internal(format!(
                "Failed to ack {} messages: {:?}",
                failed_count, errors
            )));
        }

        RpcOk!()
    }

    async fn nack_message(&self, request: RpcRequest<NackRequest>) -> RpcResponse<()> {
        let inner = request.into_inner();

        let message_id = parse_uuid(inner.message_uuid.as_slice())?;

        self.ack_manager.apply_nack(&message_id)
            .map_err(|e| Status::internal(format!("Nack failed: {:?}", e)))?;

        RpcOk!()
    }

    async fn nack_bulk(
        &self,
        request: RpcRequest<NackBulkRequest>,
    ) -> RpcResponse<()> {
        let inner = request.into_inner();
        let mut failed_count = 0;
        let mut errors = Vec::new();

        for nack_req in &inner.nack_requests {
                let message_id = parse_uuid(&nack_req.message_uuid.as_slice())?;

            if let Err(e) = self.ack_manager.apply_nack(&message_id) {
                failed_count += 1;
                errors.push(format!("{:?}", e));
            }
            // TODO: Implement requeue logic using nack_req.requeue flag
        }

        if failed_count > 0 {
            return Err(Status::internal(format!(
                "Failed to nack {} messages: {:?}",
                failed_count, errors
            )));
        }

        RpcOk!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::RuusterServer;
    use ruuster_internals::Message;
    use std::time::Duration;
    use uuid::Uuid;

    fn setup_test_server() -> RuusterServer {
        RuusterServer::new()
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
            message_uuid: msg_id.as_bytes().to_vec(),
        });

        let response = server.ack_message(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_ack_message_invalid_uuid() {
        let server = setup_test_server();

        let request = tonic::Request::new(AckRequest {
            queue_name: "test_queue".to_string(),
            message_uuid: b"invalid-uuid".to_vec(),
        });

        let response = server.ack_message(request).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_ack_message_not_found() {
        let server = setup_test_server();
        let msg_id = Uuid::new_v4();

        let request = tonic::Request::new(AckRequest {
            queue_name: "test_queue".to_string(),
            message_uuid: msg_id.as_bytes().to_vec(),
        });

        let response = server.ack_message(request).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("Ack failed"));
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
                    message_uuid: msg_id1.as_bytes().to_vec(),
                },
                AckRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id2.as_bytes().to_vec(),
                },
            ],
        });

        let response = server.ack_bulk(request).await;
        assert!(response.is_ok());
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
                    message_uuid: msg_id1.as_bytes().to_vec(),
                },
                AckRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id2.as_bytes().to_vec(), // This one doesn't exist
                },
            ],
        });

        let response = server.ack_bulk(request).await;
        assert!(response.is_err());
        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("Failed to ack 1 messages"));
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
            message_uuid: msg_id.as_bytes().to_vec(),
            requeue: true,
        });

        let response = server.nack_message(request).await;
        assert!(response.is_ok());
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
                    message_uuid: msg_id1.as_bytes().to_vec(),
                    requeue: true,
                },
                NackRequest {
                    queue_name: "test_queue".to_string(),
                    message_uuid: msg_id2.as_bytes().to_vec(),
                    requeue: false,
                },
            ],
        });

        let response = server.nack_bulk(request).await;
        assert!(response.is_ok());
    }
}
