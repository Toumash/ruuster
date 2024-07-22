use crate::Metadata;
use crate::RoutingKey;
use crate::Message;

impl From<String> for RoutingKey {
    fn from(value: String) -> Self {
        Self {
            value,
        }
    }
}

impl From<internals::Metadata> for Metadata {
    fn from(value: internals::Metadata) -> Self {
        Self {
            routing_key: value.routing_key.map(|v| v.into()),
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