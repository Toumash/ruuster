use crate::ruuster::ChatClient;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub type ServerAddr = String;
pub type RoomId = String;
pub type Nick = String;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatMessage {
    pub sender: Nick,
    pub payload: String,
}

#[derive(Debug, Default)]
pub struct Model {
    pub running_state: RunningState,
    pub server_addr: Option<ServerAddr>,
    pub room_list: Vec<RoomId>,
    pub current_room: Option<RoomId>,
    pub user_nick: Option<Nick>,
    pub messages: Vec<ChatMessage>,
    pub chat_client: ChatClient,
    pub input: String,
    pub event_sender: Option<mpsc::UnboundedSender<crate::event::Event>>,
    /// Manual scroll offset for messages (0 = auto-scroll to bottom)
    pub scroll_offset: u16,
}

impl Model {
    pub fn new() -> Self {
        Model::default()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum RunningState {
    #[default]
    StartView, // connecting to server
    RoomListView, // creating and connecting to room
    ChatView,     // sending messages
    Done,
}
