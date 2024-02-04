use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use protos::Message;
use tonic::Status;

use crate::queues::Uuid;

pub type AckContainer = HashMap<Uuid, AckRecord>;

pub struct AckRecord {
    message: Message,
    counter: isize,
    timestamp: Instant,
    duration: Duration,
}

// TODO (msaff): LATER
// used for sorting Acks by
// pub struct AckNode {
//     deadline: Instant,
//     message: Arc<Message>
// }

#[derive(Debug)]
pub enum AckStatus {
    DeadlineExceeded,
}

impl AckRecord {
    pub fn new(message: Message, timestamp: Instant, duration: Duration) -> Self {
        AckRecord {
            message,
            counter: 1,
            timestamp,
            duration,
        }
    }

    pub fn increment_counter(&mut self) {
        self.counter += 1;
    }

    pub fn is_deadline_exceeded(&self) -> bool {
        self.get_deadline() <= Instant::now()
    }

    pub fn apply_ack(&mut self) -> Result<(), AckStatus> {
        if self.is_deadline_exceeded() {
            log::info!("deadline exceeded for message: {}", self.get_message().uuid);
            return Err(AckStatus::DeadlineExceeded);
        }
        log::debug!("message acked: {}", self.message.uuid);
        self.counter -= 1;
        return Ok(());
    }

    pub fn apply_nack(&self) {}

    pub fn get_counter(&self) -> isize {
        self.counter
    }

    pub fn get_message(&self) -> &Message {
        &self.message
    }

    pub fn get_timestamp(&self) -> &std::time::Instant {
        &self.timestamp
    }

    pub fn get_deadline(&self) -> std::time::Instant {
        *self.get_timestamp() + self.duration
    }
}

pub trait ApplyAck {
    fn apply_ack(&mut self, uuid: &Uuid) -> Result<(), Status>;
    fn clear_unused_record(&mut self, uuid: &Uuid) -> Result<(), Status>;
}

impl ApplyAck for AckContainer {
    fn clear_unused_record(&mut self, uuid: &Uuid) -> Result<(), Status> {
        if let Some(record) = self.get(uuid) {
            if record.get_counter() <= 0 {
                log::debug!("deleting ack record for message with uuid: {}", uuid);
                self.remove(uuid);
            }
            return Ok(());
        }
        let msg = format!("ack record for message with uuid: {} not found", uuid);
        log::debug!("{}", &msg);
        Err(Status::not_found(&msg))
    }

    fn apply_ack(&mut self, uuid: &Uuid) -> Result<(), Status> {
        if let Some(record) = self.get_mut(uuid) {
            record.apply_ack().map_err(|e| {
                let msg = format!("appling ack for message failed: {:?}", e);
                log::error!("{}", &msg);
                Status::internal(&msg)
            })?;
            return Ok(());
        }
        let msg = format!("ack record for message with uuid: {} not found", uuid);
        log::debug!("{}", &msg);
        Err(Status::not_found(&msg))
    }
}
