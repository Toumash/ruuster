use std::{
    sync::Arc,
    time::{Instant, SystemTime},
};
use uuid::Uuid;

#[derive(Clone, PartialEq, Default, Debug)]
pub struct Message {
    pub uuid: Uuid,
    pub routing_key: Option<String>,
    pub payload: Arc<Vec<u8>>,
    pub metadata: Option<MessageMetadata>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct MessageMetadata {
    pub client_time: Option<SystemTime>,
    pub arrival_time: Option<Instant>,
}
