#![allow(dead_code)] // disable false-positive warnings

use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub server_metadata: ServerMetadata,
    pub queues: Vec<Queue>,
    pub exchanges: Vec<Exchange>,
    pub producers: Vec<Producer>,
    pub consumers: Vec<Consumer>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerMetadata {
    pub name: String,
    pub server_addr: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    pub name: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    pub name: String,
    pub kind: i32,
    pub bindings: Vec<Binding>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    pub queue_name: String,
    pub bind_metadata: Option<BindMetadata>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BindMetadata {
    pub routing_key: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Producer {
    pub name: String,
    pub destination: String,
    pub messages_produced: i32,
    pub message_payload_bytes: i32,
    pub post_message_delay_ms: i32,
    pub metadata: Option<Metadata>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub routing_key: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Consumer {
    pub name: String,
    pub source: String,
    pub consuming_method: String,
    pub ack_method: String,
    pub workload_ms: WorkloadMs,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadMs {
    pub min: i32,
    pub max: i32,
}
