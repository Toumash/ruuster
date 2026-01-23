use std::sync::Arc;

use crate::errors::RuusterError;
use crate::structs::{Message, MessageMetadata};
use ruuster_protos::v1::{Message as ProtoMsg, MessageMetadata as ProtoMeta};
use uuid::Uuid;

// ==========================================
// 1. INGRESS: PROTO -> INTERNAL (Validation)
// ==========================================

impl TryFrom<ProtoMsg> for Message {
    type Error = RuusterError;

    fn try_from(proto: ProtoMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            // MATURE: Validate UUID. Do not invent one.
            uuid: Uuid::from_slice(&proto.uuid)
                .map_err(|_| RuusterError::InvalidMetadata("Invalid or missing UUID".into()))?,

            routing_key: proto.routing_key,
            payload: Arc::new(proto.payload),

            // Map metadata if present
            metadata: proto.metadata.map(|m| m.into()),
        })
    }
}

impl From<ProtoMeta> for MessageMetadata {
    fn from(proto: ProtoMeta) -> Self {
        Self {
            client_time: proto.client_time.and_then(|ts| {
                let dur = std::time::Duration::new(ts.seconds as u64, ts.nanos as u32);
                std::time::UNIX_EPOCH.checked_add(dur)
            }),
            arrival_time: None, // Purposely left empty for the Queue to fill
        }
    }
}

// ==========================================
// 2. EGRESS: INTERNAL -> PROTO (Translation)
// ==========================================

impl From<Message> for ProtoMsg {
    fn from(internal: Message) -> Self {
        Self {
            uuid: internal.uuid.as_bytes().to_vec(),
            routing_key: internal.routing_key,
            payload: (*internal.payload).clone(),
            metadata: internal.metadata.map(|m| m.into()),
        }
    }
}

impl From<MessageMetadata> for ProtoMeta {
    fn from(internal: MessageMetadata) -> Self {
        Self {
            client_time: internal.client_time.and_then(|st| {
                st.duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .map(|dur| prost_types::Timestamp {
                        seconds: dur.as_secs() as i64,
                        nanos: dur.subsec_nanos() as i32,
                    })
            }),
        }
    }
}
