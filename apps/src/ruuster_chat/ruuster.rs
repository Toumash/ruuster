use exchanges::ExchangeKind;
use protos::{
    ruuster_client::RuusterClient, BindRequest, ExchangeDeclareRequest, ExchangeDefinition,
    ProduceRequest, QueueDeclareRequest,
};
use std::fmt::Display;
use thiserror::Error;
use tonic::transport::Channel;

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

#[derive(Default, Debug)]
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
        client.exchange_declare(create_exchange_request).await?;
        Ok(())
    }

    pub async fn connect_to_room(
        &mut self,
        room_id: RoomId,
        nick: Nick,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let create_queue_request = QueueDeclareRequest {
            queue_name: nick.clone(),
        };
        let bind_request = BindRequest {
            metadata: None,
            exchange_name: room_id,
            queue_name: nick,
        };

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| ChatRuusterClientError::Disconnected)?;
        client.queue_declare(create_queue_request).await?;
        client.bind(bind_request).await?;

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
