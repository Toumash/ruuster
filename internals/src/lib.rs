use std::time::Duration;

#[derive(Clone, PartialEq, Default, Debug)]
pub struct Message {
    pub uuid: String,
    pub payload: String,
    pub metadata: Option<Metadata>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Metadata {
    pub routing_key: Option<String>,
    pub created_at: Duration, // since UNIX_EPOCH
    pub dead_letter: Option<DeadLetterMetadata>,
    pub persistent: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub struct DeadLetterMetadata {
    pub count: i64,
    pub exchange: String,
    pub queue: String,
}
