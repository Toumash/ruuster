use crate::ruuster::ChatClient;
use serde_derive::{Deserialize, Serialize};

pub type ServerAddr  = String;
pub type RoomId      = String;
pub type Nick        = String;


#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    sender: Nick,
    payload: String
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
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum RunningState {
    #[default]
    StartView, // connecting to server
    RoomListView, // creating and connecting to room
    ChatView, // sending messages
    Done,
}

