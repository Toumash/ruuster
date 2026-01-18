use crate::model::Model;
use crate::update::{update, ChatEvent};

pub struct App {
    pub model: Model,
}

impl App {
    pub fn new() -> Self {
        App {
            model: Model::new(),
        }
    }

    pub async fn update(&mut self, event: ChatEvent) -> Result<(), Box<dyn std::error::Error>> {
        update(&mut self.model, event).await
    }
}
