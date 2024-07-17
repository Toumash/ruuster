use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use internals::Message;
use tonic::Status;

use tracing::{debug, error, info, instrument, warn};

type UuidSerialized = String;

pub type AckContainer = HashMap<UuidSerialized, AckRecord>;

pub struct AckRecord {
    message: Message,
    counter: isize,
    timestamp: Instant,
    duration: Duration,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AckStatus {
    DeadlineExceeded,
    MessageAlreadyAcked,
}

pub trait ApplyAck {
    fn apply_ack(&mut self, uuid: &UuidSerialized) -> Result<(), Status>;
    fn apply_bulk_ack(&mut self, uuids: &[UuidSerialized]) -> Result<(), Status>;
    fn clear_unused_record(&mut self, uuid: &UuidSerialized) -> Result<(), Status>;
    fn clear_all_unused_records(&mut self) -> Result<(), Status>;
    fn add_record(&mut self, message: Message, duration: Duration);
}

impl ApplyAck for AckContainer {
    #[instrument(skip_all, fields(uuid=%uuid))]
    fn clear_unused_record(&mut self, uuid: &UuidSerialized) -> Result<(), Status> {
        if let Some(record) = self.get(uuid) {
            if record.get_counter() <= 0 {
                info!("deleting ack record");
                self.remove(uuid);
            }
            return Ok(());
        }
        warn!("ack record not found");
        Err(Status::not_found("ack record not found"))
    }

    #[instrument(skip_all, fields(uuid=%message.uuid, duartion=?duration))]
    fn add_record(&mut self, message: Message, duration: Duration) {
        let uuid = message.uuid.to_owned();
        info!("adding ack record");
        self.entry(uuid)
            .and_modify(|elem| {
                elem.increment_counter();
                info!("ack record incremented");
            })
            .or_insert(AckRecord::new(message, Instant::now(), duration));
    }

    #[instrument(skip_all, fields(uuid=%uuid))]
    fn apply_ack(&mut self, uuid: &UuidSerialized) -> Result<(), Status> {
        if let Some(record) = self.get_mut(uuid) {
            record.apply_ack().map_err(|e| {
                error!(error=?e, "appling ack failed");
                Status::internal("appling ack failed")
            })?;
            info!("ack applied");
            return Ok(());
        }
        warn!("ack record not found");
        Err(Status::not_found("ack record not found"))
    }

    #[instrument(skip_all, fields(uuids=?uuids))]
    fn apply_bulk_ack(&mut self, uuids: &[UuidSerialized]) -> Result<(), Status> {
        for item in uuids {
            // note (msaff): we can safely ignore errors from apply_ack - we want to continue to ack messages from array
            let _ = self.apply_ack(item);
        }
        Ok(())
    }

    #[instrument(skip_all)]
    fn clear_all_unused_records(&mut self) -> Result<(), Status> {
        self.retain(|_, value| value.get_counter() > 0);
        Ok(())
    }
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
            info!("deadline exceeded for message: {}", self.get_message().uuid);
            return Err(AckStatus::DeadlineExceeded);
        }
        if self.counter <= 0 {
            info!("message {} already acked", self.get_message().uuid);
            return Err(AckStatus::MessageAlreadyAcked);
        }

        debug!("message acked: {}", self.message.uuid);
        self.counter -= 1;
        Ok(())
    }

    pub fn apply_nack(&self) {}

    pub fn get_counter(&self) -> isize {
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

#[test]
fn ack_record_sample_track_message_delivery() {
    let mut acks = AckContainer::new();
    let msg = Message {
        uuid: "teepot".to_string(),
        payload: "#badcaffe".to_string(),
        metadata: None,
    }; // fake message with hardcoded uuid

    acks.add_record(msg.clone(), Duration::from_millis(100));
    assert_eq!(acks.len(), 1); // first call creates an entry in map

    acks.add_record(msg.clone(), Duration::from_millis(100));
    assert_eq!(acks.len(), 1); // second call with the same uuid updates an existing entry

    let msg = Message {
        uuid: "teepot2".to_string(),
        payload: "#badcaffe".to_string(),
        metadata: None,
    };
    acks.add_record(msg.clone(), Duration::from_millis(100));
    assert_eq!(acks.len(), 2); // message with different uuid creates a separate entry

    assert!(acks.apply_ack(&"teepot".to_string()).is_ok()); // this will decrement ack record of teepot by one
    assert_eq!(acks.len(), 2); // all records are still in container

    assert!(acks.clear_unused_record(&"teepot".to_string()).is_ok()); // teepot msg is still used
    assert_eq!(acks.len(), 2); // so no record is removed

    assert!(acks.apply_ack(&"teepot".to_string()).is_ok()); // now couter for teepot msg is 0
    assert!(acks.apply_ack(&"teepot".to_string()).is_err()); // cannot apply ack to zeroed record

    assert!(acks.clear_unused_record(&"teepot".to_string()).is_ok()); // lets now remove unused teepot msg record
    assert!(acks.clear_unused_record(&"teepot".to_string()).is_err()); // teepot msg record already removed
    assert_eq!(acks.len(), 1); // confirm that teepot msg record no longer exists

    assert!(acks.apply_ack(&"teepot2".to_string()).is_ok()); // lets ack second msg
    assert!(acks.clear_unused_record(&"teepot2".to_string()).is_ok());
    assert_eq!(acks.len(), 0);

    let msg1 = Message {
        uuid: "teepot".to_string(),
        payload: "#badcaffe".to_string(),
        metadata: None,
    };
    let msg2 = Message {
        uuid: "teepot2".to_string(),
        payload: "#badcaffe".to_string(),
        metadata: None,
    };
    acks.add_record(msg1.clone(), Duration::from_millis(100));
    acks.add_record(msg2.clone(), Duration::from_millis(100));
    assert_eq!(acks.len(), 2);
    assert!(acks
        .apply_bulk_ack(&["teepot".to_string(), "teepot2".to_string()])
        .is_ok());
    assert!(acks.clear_all_unused_records().is_ok());
    assert_eq!(acks.len(), 0, "lipa ale wiadomo gdzie");
}

#[test]
fn ack_record_sample_controlling_collection() {
    let mut ack_record = AckRecord::new(
        Message::default(),
        Instant::now(),
        Duration::from_millis(100),
    );
    std::thread::sleep(Duration::from_millis(50));
    assert!(ack_record.apply_ack().is_ok());

    let mut ack_record = AckRecord::new(
        Message::default(),
        Instant::now(),
        Duration::from_millis(50),
    );
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        ack_record.apply_ack().unwrap_err(),
        AckStatus::DeadlineExceeded
    );

    let mut ack_record = AckRecord::new(
        Message::default(),
        Instant::now(),
        Duration::from_millis(50),
    );
    ack_record.increment_counter();
    assert!(ack_record.apply_ack().is_ok());
    assert!(ack_record.apply_ack().is_ok());
    assert_eq!(
        ack_record.apply_ack().unwrap_err(),
        AckStatus::MessageAlreadyAcked
    );
}
