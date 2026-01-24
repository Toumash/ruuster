use dashmap::DashMap;
use ruuster_internals::Message;
use std::time::Duration;
use uuid::Uuid;

pub struct AckRecord {
    message: Message,
    timestamp: std::time::Instant,
    counter: i64,
    duration: Duration,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AckStatus {
    DeadlineExceeded,
    MessageAlreadyAcked,
}

impl AckRecord {
    pub fn new(message: Message, timestamp: std::time::Instant, duration: Duration) -> Self {
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
        self.get_deadline() <= std::time::Instant::now()
    }

    pub fn apply_ack(&mut self) -> Result<(), AckStatus> {
        if self.is_deadline_exceeded() {
            return Err(AckStatus::DeadlineExceeded);
        }
        if self.counter <= 0 {
            return Err(AckStatus::MessageAlreadyAcked);
        }

        self.counter -= 1;
        Ok(())
    }

    pub fn apply_nack(&self) {}

    pub fn into_message(self) -> Message {
        self.message
    }

    pub fn get_counter(&self) -> i64 {
        self.counter
    }

    pub fn get_message(&self) -> &Message {
        &self.message
    }

    pub fn get_timestamp(&self) -> std::time::Instant {
        self.timestamp
    }

    pub fn get_deadline(&self) -> std::time::Instant {
        self.get_timestamp() + self.duration
    }
}

pub struct AckManager {
    records: DashMap<Uuid, AckRecord>,
}

impl AckManager {
    pub fn new() -> Self {
        Self {
            records: DashMap::new(),
        }
    }
    pub fn add_record(&self, record: AckRecord) {
        self.records.insert(record.message.uuid, record);
    }
    pub fn apply_ack(&self, message_id: &Uuid) -> Result<(), AckStatus> {
        let mut record = self.records.get_mut(message_id).ok_or_else(|| {
            AckStatus::MessageAlreadyAcked // or some other error indicating not found
        })?;

        record.apply_ack()?;

        if record.get_counter() == 0 {
            self.records.remove(message_id);
        }

        Ok(())
    }
    pub fn apply_nack(&self, message_id: &Uuid) -> Result<(), AckStatus> {
        let record = self.records.get(message_id).ok_or_else(|| {
            AckStatus::MessageAlreadyAcked // or some other error indicating not found
        })?;

        record.apply_nack();
        Ok(())
    }
    pub fn remove_record(&self, message_id: &Uuid) {
        self.records.remove(message_id);
    }
    pub fn cleanup_all_expired(&self) {
        self.records
            .retain(|_, record| !record.is_deadline_exceeded());
    }
    pub fn cleanup_all_acked(&self) {
        self.records
            .retain(|_, record| record.get_counter() > 0);
    }
}
