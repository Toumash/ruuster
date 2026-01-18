use exchanges::ExchangeKind;
use protos::{
    ruuster_client::RuusterClient, BindRequest, ConsumeRequest, ExchangeDeclareRequest,
    ExchangeDefinition, ProduceRequest, QueueDeclareRequest, RemoveQueueRequest, UnbindRequest,
};
use std::fmt::Display;
use thiserror::Error;
use tonic::transport::Channel;
use tonic::Streaming;

use crate::model::*;

#[derive(Debug, Error)]
pub enum ChatRuusterClientError {
    Disconnected,
}

impl Display for ChatRuusterClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            ChatRuusterClientError::Disconnected => write!(f, "client is not connected to Ruuster"),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct ChatClient {
    client: Option<RuusterClient<Channel>>,
}

impl ChatClient {
    pub async fn connect(
        &mut self,
        server_addr: ServerAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = RuusterClient::connect(server_addr).await?;
        self.client = Some(client);
        Ok(())
    }

    pub fn disconnect(&mut self) {
        self.client = None;
    }

    /// Creates a room (exchange). Ignores "already exists" errors.
    pub async fn create_room(&mut self, room_id: RoomId) -> Result<(), Box<dyn std::error::Error>> {
        let create_exchange_request = ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout.into(),
                exchange_name: room_id,
            }),
        };
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| ChatRuusterClientError::Disconnected)?;

        // Ignore "already exists" error - room may have been created by another user
        let _ = client.exchange_declare(create_exchange_request).await;
        Ok(())
    }

    pub async fn connect_to_room(
        &mut self,
        room_id: RoomId,
        nick: Nick,
    ) -> Result<Streaming<protos::Message>, Box<dyn std::error::Error>> {
        // Create user queue
        let create_queue_request = QueueDeclareRequest {
            queue_name: nick.clone(),
        };

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| ChatRuusterClientError::Disconnected)?;

        // We ignore error here as queue might already exist
        let _ = client.queue_declare(create_queue_request).await;

        // Bind user queue to room exchange
        let bind_request = BindRequest {
            metadata: None,
            exchange_name: room_id.clone(),
            queue_name: nick.clone(),
        };
        client.bind(bind_request).await?;

        // Start consuming
        let consume_request = ConsumeRequest {
            queue_name: nick,
            auto_ack: true,
        };

        let response = client.consume_bulk(consume_request).await?;
        Ok(response.into_inner())
    }

    pub async fn exit_room(
        &mut self,
        room_id: RoomId,
        nick: Nick,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| ChatRuusterClientError::Disconnected)?;

        // Unbind user queue from room exchange
        let unbind_request = UnbindRequest {
            metadata: None,
            exchange_name: room_id,
            queue_name: nick.clone(),
        };
        let _ = client.unbind(unbind_request).await;

        // Remove user queue
        let remove_queue_request = RemoveQueueRequest { queue_name: nick };
        let _ = client.remove_queue(remove_queue_request).await;

        Ok(())
    }

    pub async fn send_message(
        &mut self,
        chat_message: ChatMessage,
        room_id: RoomId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let produce_request = ProduceRequest {
            metadata: None,
            exchange_name: room_id,
            payload: serde_json::to_string(&chat_message)?,
        };

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| ChatRuusterClientError::Disconnected)?;

        client.produce(produce_request).await?;

        Ok(())
    }
}
