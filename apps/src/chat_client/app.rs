use protos::{ruuster_client::RuusterClient, ruuster_server::Ruuster};
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

#[derive(Debug, Default, PartialEq, Eq)]
pub enum AppState {
    #[default]
    InputName,
    Running,
    Help,
    Done,
}

#[derive(Debug, Default)]
pub struct App {
    pub state: AppState,
    pub user_name: String,
    pub input_message: String,
    messages: Vec<ChatMessage>,
    auto_scroll: bool,
    visible_messages_boundries: (usize, usize),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub author: String,
    pub content: String,
}

impl App {
    pub fn new() -> Self {
        let mut app = App::default();

        app.messages.push(ChatMessage {
            author: "Ruuster".to_string(),
            content: "Welcome to RuusterChat".to_string(),
        });

        app.visible_messages_boundries = (0, std::cmp::min(10, app.messages.len() - 1));

        app
    }

    pub fn quit(&mut self) {
        self.state = AppState::Done;
    }

    pub fn pull_message(&mut self, msg: ChatMessage) {
        self.messages.push(msg);
        self.visible_messages_boundries = (0, std::cmp::min(10, self.messages.len() - 1));
    }

    pub fn validate_user_name(&self) -> bool {
        if self.user_name.len() > 0 {
            return true;
        }
        false
    }

    pub fn is_done(&self) -> bool {
        self.state == AppState::Done
    }

    pub fn scroll_up(&mut self) {
        let (mut begin, mut end) = self.visible_messages_boundries;
        if begin <= 0 {
            return;
        }
        begin -= 1;
        end -= 1;
        self.visible_messages_boundries = (begin, end)
    }

    pub fn scroll_down(&mut self) {
        let (mut begin, mut end) = self.visible_messages_boundries;
        if end >= self.messages.len() - 1 {
            return;
        }
        begin += 1;
        end += 1;
        self.visible_messages_boundries = (begin, end)
    }

    pub fn get_visible_messages(&self) -> &[ChatMessage] {
        let (begin, end) = self.visible_messages_boundries;
        &self.messages[begin..=end]
    }
}
