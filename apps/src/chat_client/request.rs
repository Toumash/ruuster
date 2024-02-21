use exchanges::ExchangeKind;
use protos::{
    ruuster, ruuster_client::RuusterClient, BindQueueToExchangeRequest, ConsumeRequest, Empty,
    ExchangeDeclareRequest, ExchangeDefinition, QueueDeclareRequest,
};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::app::{App, ChatMessage};

const EXCHANGE_NAME: &str = "ruuster.test.chat";

async fn setup_exchange(
    client: &mut RuusterClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.list_exchanges(Empty {}).await?;
    let response = response.into_inner();

    if response.exchange_names.contains(&EXCHANGE_NAME.to_string()) {
        return Ok(());
    }

    client
        .exchange_declare(ExchangeDeclareRequest {
            exchange: Some(ExchangeDefinition {
                kind: ExchangeKind::Fanout as i32,
                exchange_name: EXCHANGE_NAME.to_string(),
            }),
        })
        .await?;

    Ok(())
}

async fn connect_user(
    client: &mut RuusterClient<Channel>,
    user_name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .bind_queue_to_exchange(BindQueueToExchangeRequest {
            queue_name: user_name,
            exchange_name: EXCHANGE_NAME.to_string(),
        })
        .await?;
    Ok(())
}

pub async fn setup_user(
    client: &mut RuusterClient<Channel>,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.list_queues(Empty {}).await?;
    let response = response.into_inner();

    if response.queue_names.contains(&app.user_name) {
        return Err("user with this name already exists".into());
    }

    client
        .queue_declare(QueueDeclareRequest {
            queue_name: app.user_name.clone(),
        })
        .await?;

    setup_exchange(client).await?;
    connect_user(client, app.user_name.clone()).await?;

    Ok(())
}

pub async fn send_message(
    client: &mut RuusterClient<Channel>,
    message: ChatMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let serialized = serde_json::to_string(&message)?;

    let ruuster_msg = ruuster::Message {
        uuid: Uuid::new_v4().to_string(),
        payload: serialized,
    };

    let request = ruuster::ProduceRequest {
        payload: Some(ruuster_msg),
        exchange_name: EXCHANGE_NAME.to_string(),
    };

    client.produce(request).await?;

    Ok(())
}

pub async fn listen_for_message(
    client: &mut RuusterClient<Channel>,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = ConsumeRequest {
        queue_name: app.user_name.clone(),
        auto_ack: true,
    };

    let response = client.consume_one(request).await?;
    let response = response.into_inner();

    let serialized = response.payload;

    let chat_message: ChatMessage = serde_json::from_str(&serialized)?;
    app.pull_message(chat_message);

    Ok(())
}
