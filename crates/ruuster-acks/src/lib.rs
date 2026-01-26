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
    AckRecordNotFound,
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
    pub fn add_record(&self, message: Message, duration: Duration) {
        let key = message.uuid;
        self.records
            .entry(key)
            .and_modify(|rec| {
                rec.increment_counter();
            })
            .or_insert(AckRecord::new(message, std::time::Instant::now(), duration));
    }
    pub fn apply_ack(&self, message_id: &Uuid) -> Result<(), AckStatus> {
        let mut record = self
            .records
            .get_mut(message_id)
            .ok_or_else(|| AckStatus::AckRecordNotFound)?;

        record.apply_ack()?;

        Ok(())
    }
    pub fn apply_nack(&self, message_id: &Uuid) -> Result<(), AckStatus> {
        let record = self
            .records
            .get(message_id)
            .ok_or_else(|| AckStatus::AckRecordNotFound)?;

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
        self.records.retain(|_, record| record.get_counter() > 0);
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use ruuster_internals::Message;
    use std::{thread, time::Duration};
    use uuid::Uuid;

    fn create_message() -> Message {
        Message {
            uuid: Uuid::new_v4(),
            ..Default::default()
        }
    }

    #[test]
    fn test_ack_record_lifecycle() {
        let message = create_message();
        let timestamp = std::time::Instant::now();
        let duration = Duration::from_secs(60);
        let mut record = AckRecord::new(message.clone(), timestamp, duration);

        assert_eq!(record.get_counter(), 1);
        assert!(!record.is_deadline_exceeded());

        // Increment counter
        record.increment_counter();
        assert_eq!(record.get_counter(), 2);

        // Apply ack
        assert!(record.apply_ack().is_ok());
        assert_eq!(record.get_counter(), 1);

        // Apply ack again
        assert!(record.apply_ack().is_ok());
        assert_eq!(record.get_counter(), 0);

        // Apply ack on already acked (counter <= 0)
        assert_eq!(record.apply_ack(), Err(AckStatus::MessageAlreadyAcked));
    }

    #[test]
    fn test_ack_record_expiration() {
        let message = create_message();
        let timestamp = std::time::Instant::now();
        let duration = Duration::from_millis(10); // Short duration
        let mut record = AckRecord::new(message, timestamp, duration);

        // Wait for expiration
        thread::sleep(Duration::from_millis(20));
        assert!(record.is_deadline_exceeded());

        // Try to ack expired record
        assert_eq!(record.apply_ack(), Err(AckStatus::DeadlineExceeded));
    }

    #[test]
    fn test_ack_manager_flow() {
        let manager = AckManager::new();
        let message = create_message();
        let msg_id = message.uuid;
        let duration = Duration::from_secs(60);

        // Add record
        manager.add_record(message.clone(), duration);
        
        assert!(manager.apply_ack(&msg_id).is_ok());
        
        // It had counter 1, so now it should be 0. 
        // Applying again should fail with MessageAlreadyAcked
        assert_eq!(manager.apply_ack(&msg_id), Err(AckStatus::MessageAlreadyAcked));

        // Add same record again -> increments counter to 1 (if it was 0)
        manager.add_record(message, duration);
        
        assert!(manager.apply_ack(&msg_id).is_ok());
    }

    #[test]
    fn test_ack_manager_cleanup() {
        let manager = AckManager::new();
        let msg1 = create_message();
        let msg2 = create_message();
        let duration_long = Duration::from_secs(60);
        let duration_short = Duration::from_millis(10);

        // msg1: Acked (counter 0)
        manager.add_record(msg1.clone(), duration_long);
        manager.apply_ack(&msg1.uuid).unwrap(); // counter 0
        
        // msg2: Expired
        manager.add_record(msg2.clone(), duration_short);
        thread::sleep(Duration::from_millis(20));

        // Cleanup acked
        manager.cleanup_all_acked();
        assert_eq!(manager.apply_ack(&msg1.uuid), Err(AckStatus::AckRecordNotFound));

        // msg2 is still there (expired but not acked fully/removed)
        // Cleanup expired
        manager.cleanup_all_expired();
         
        assert_eq!(manager.apply_ack(&msg2.uuid), Err(AckStatus::AckRecordNotFound));
    }
}
