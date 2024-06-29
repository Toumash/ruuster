use std::time::Instant;

#[derive(Clone, PartialEq, Default, Debug)]
pub struct Message {
    pub uuid: String,
    pub payload: String,
    pub metadata: Option<Metadata>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Metadata {
    pub routing_key: Option<String>,
    pub created_at: Option<Instant>,
    pub dead_letter: Option<DeadLetterMetadata>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct DeadLetterMetadata {
    pub count: Option<i64>,
    pub exchange: Option<String>,
    pub queue: Option<String>,
}
