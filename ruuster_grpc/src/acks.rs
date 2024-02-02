use std::time::{Duration, Instant};

use protos::Message;

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
    Ok,
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
        self.timestamp + self.duration <= Instant::now()
    }

    pub fn apply_ack(&mut self) -> Result<(), AckStatus> {
        if self.is_deadline_exceeded() {
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
        self.timestamp + self.duration
    }
}
