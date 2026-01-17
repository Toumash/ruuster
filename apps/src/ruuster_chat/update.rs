use crate::model::*;
use std::fmt::Display;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum ChatEvent {
    ConnectToServer(ServerAddr),
    CreateRoom(RoomId),
    Disconnect,
    ConnectToRoom(RoomId, Nick),
    ExitRoom,
    SendMessage(ChatMessage),
    ReceiveMessage(ChatMessage),
    Quit,
    SetSender(mpsc::UnboundedSender<crate::event::Event>),
}

#[derive(Debug, Error)]
pub enum ChatUpdateError {
    NotInRoom,
}

impl Display for ChatUpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatUpdateError::NotInRoom => write!(f, "client is not connected to a room"),
        }
    }
}

async fn connect_to_server(
    server_addr: ServerAddr,
    model: &mut Model,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(model.running_state, RunningState::StartView);
    model.chat_client.connect(server_addr.clone()).await?;
    model.server_addr = Some(server_addr);
    model.running_state = RunningState::RoomListView;
    Ok(())
}

fn disconnect(model: &mut Model) {
    assert_eq!(model.running_state, RunningState::RoomListView);
    model.chat_client.disconnect();
    model.server_addr = None;
    model.running_state = RunningState::StartView;
}

async fn create_room(room_id: RoomId, model: &mut Model) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(model.running_state, RunningState::RoomListView);
    model.chat_client.create_room(room_id.clone()).await?;
    model.room_list.push(room_id);
    Ok(())
}

async fn connect_to_room(
    room_id: RoomId,
    nick: Nick,
    model: &mut Model,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(model.running_state, RunningState::RoomListView);

    let mut streaming = model
        .chat_client
        .connect_to_room(room_id.clone(), nick.clone())
        .await?;

    model.current_room = Some(room_id);
    model.user_nick = Some(nick);
    model.running_state = RunningState::ChatView;
    model.messages.clear();

    if let Some(sender) = model.event_sender.clone() {
        tokio::spawn(async move {
            while let Ok(Some(msg)) = streaming.message().await {
                if let Ok(chat_msg) = serde_json::from_str::<ChatMessage>(&msg.payload) {
                    let _ = sender.send(crate::event::Event::ChatEvent(ChatEvent::ReceiveMessage(
                        chat_msg,
                    )));
                }
            }
        });
    }

    Ok(())
}

async fn exit_room(model: &mut Model) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(model.running_state, RunningState::ChatView);
    if let (Some(room_id), Some(nick)) = (&model.current_room, &model.user_nick) {
        model
            .chat_client
            .exit_room(room_id.clone(), nick.clone())
            .await?;
    }
    model.current_room = None;
    model.running_state = RunningState::RoomListView;
    Ok(())
}

async fn send_message(
    message: ChatMessage,
    model: &mut Model,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(model.running_state, RunningState::ChatView);
    let room_id = model
        .current_room
        .as_ref()
        .ok_or_else(|| ChatUpdateError::NotInRoom)?;
    model
        .chat_client
        .send_message(message, room_id.clone())
        .await?;
    Ok(())
}

fn receive_message(message: ChatMessage, model: &mut Model) {
    if model.running_state == RunningState::ChatView {
        model.messages.push(message);
    }
}

fn quit(model: &mut Model) {
    model.running_state = RunningState::Done;
}

pub async fn update(model: &mut Model, event: ChatEvent) -> Result<(), Box<dyn std::error::Error>> {
    match event {
        ChatEvent::ConnectToServer(ip_addr) => connect_to_server(ip_addr, model).await?,
        ChatEvent::CreateRoom(room_id) => create_room(room_id, model).await?,
        ChatEvent::ConnectToRoom(room_id, nick) => connect_to_room(room_id, nick, model).await?,
        ChatEvent::ExitRoom => exit_room(model).await?,
        ChatEvent::SendMessage(message) => send_message(message, model).await?,
        ChatEvent::ReceiveMessage(message) => receive_message(message, model),
        ChatEvent::Quit => quit(model),
        ChatEvent::Disconnect => disconnect(model),
        ChatEvent::SetSender(sender) => {
            model.event_sender = Some(sender);
        }
    }
    Ok(())
}
