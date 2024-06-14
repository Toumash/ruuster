use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioConfig {
    pub metadata: Metadata,
    pub queues: Vec<Queue>,
    pub exchanges: Vec<Exchange>,
    pub producers: Vec<Producer>,
    pub consumers: Vec<Consumer>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
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
    pub metadata: Value,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Producer {
    pub name: String,
    pub destination: String,
    pub messages_produced: i32,
    pub message_payload_bytes: i32,
    pub post_message_delay_ms: f32,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Consumer {
    pub name: String,
    pub source: String,
    pub consuming_method: String,
    pub ack_method: String,
    pub workload_ms: WorkloadSeconds,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadSeconds {
    pub min: i32,
    pub max: i32,
}
