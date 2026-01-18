use std::time::Duration;

use crate::Metadata;
use crate::RoutingKey;
use crate::Message;
use crate::DeadLetterMetadata;

impl From<String> for RoutingKey {
    fn from(value: String) -> Self {
        Self { value }
    }
}

impl From<internals::DeadLetterMetadata> for DeadLetterMetadata {
    fn from(value: internals::DeadLetterMetadata) -> Self {
        Self {
            count: value.count,
            exchange: value.exchange,
            queue: value.queue,
        }
    }
}

impl From <DeadLetterMetadata> for internals::DeadLetterMetadata {
    fn from(value: DeadLetterMetadata) -> Self {
        Self {
            count: value.count,
            exchange: value.exchange,
            queue: value.queue,
        }
    }
}

impl From<internals::Metadata> for Metadata {
    fn from(value: internals::Metadata) -> Self {
        Self {
            routing_key: value.routing_key.map(|v| v.into()),
            created_at: value.created_at.as_millis() as i64,
            persistent: value.persistent,
            dead_letter: value.dead_letter.map(|dl| dl.into()),
        }
    }
}

impl From<Metadata> for internals::Metadata {
    fn from(value: Metadata) -> Self {
        Self {
            routing_key: value.routing_key.map(|v| v.value),
            created_at: Duration::from_millis(value.created_at as u64),
            dead_letter: value.dead_letter.map(|dl| dl.into()),
            persistent: value.persistent,
        }
    }
}

impl From<internals::Message> for Message {
    fn from(value: internals::Message) -> Self {
        Self {
            uuid: value.uuid,
            payload: value.payload,
            metadata: value.metadata.map(|v| v.into()),
        }
    }
}
