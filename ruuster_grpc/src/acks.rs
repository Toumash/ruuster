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

#[derive(Debug, Eq, PartialEq)]
pub enum AckStatus {
    DeadlineExceeded,
    MessageAlreadyAcked,
}

/// Can be used to track message delivery
/// # Examples
///
/// ```
/// use ruuster_grpc::acks::{AckRecord, AckStatus};
/// use protos::Message;
/// use std::time::{Duration, Instant};
///
/// let mut ack_record = AckRecord::new(Message::default(), Instant::now(), Duration::from_millis(100));
/// std::thread::sleep(Duration::from_millis(50));
/// assert!(ack_record.apply_ack().is_ok());
///
/// let mut ack_record = AckRecord::new(Message::default(), Instant::now(), Duration::from_millis(50));
/// std::thread::sleep(Duration::from_millis(100));
/// assert_eq!(ack_record.apply_ack().unwrap_err(), AckStatus::DeadlineExceeded);
///
/// let mut ack_record = AckRecord::new(Message::default(), Instant::now(), Duration::from_millis(50));
/// ack_record.increment_counter();
/// assert!(ack_record.apply_ack().is_ok());
/// assert!(ack_record.apply_ack().is_ok());
/// assert_eq!(ack_record.apply_ack().unwrap_err(), AckStatus::MessageAlreadyAcked);
/// ```
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
        if self.counter <= 0 {
            log::info!("message {} already acked", self.get_message().uuid);
            return Err(AckStatus::MessageAlreadyAcked);
        }

        log::debug!("message acked: {}", self.message.uuid);
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

/// AckContainer has few methods for controlling AckRecords collection
/// # Examples:
/// ```
/// use ruuster_grpc::acks::{AckContainer, AckStatus, ApplyAck};
/// use protos::Message;
/// use std::time::{Duration, Instant};
///
/// let mut acks = AckContainer::new();
/// let msg = Message { uuid : "teepot".to_string(), payload: "#badcaffe".to_string()}; // fake message with hardcoded uuid
///
/// acks.add_record(msg.clone(), Duration::from_millis(100));
/// assert_eq!(acks.len(), 1); // first call creates an entry in map
///
/// acks.add_record(msg.clone(), Duration::from_millis(100));
/// assert_eq!(acks.len(), 1); // second call with the same uuid updates an existing entry
///
/// let msg = Message { uuid : "teepot2".to_string(), payload: "#badcaffe".to_string()};
/// acks.add_record(msg.clone(), Duration::from_millis(100));
/// assert_eq!(acks.len(), 2); // message with different uuid creates a separate entry
///
/// assert!(acks.apply_ack(&"teepot".to_string()).is_ok()); // this will decrement ack record of teepot by one
/// assert_eq!(acks.len(), 2); // all records are still in container
///
/// assert!(acks.clear_unused_record(&"teepot".to_string()).is_ok()); // teepot msg is still used
/// assert_eq!(acks.len(), 2);                                       // so no record is removed
///
/// assert!(acks.apply_ack(&"teepot".to_string()).is_ok());  // now couter for teepot msg is 0
/// assert!(acks.apply_ack(&"teepot".to_string()).is_err()); // cannot apply ack to zeroed record
///
/// assert!(acks.clear_unused_record(&"teepot".to_string()).is_ok()); // lets now remove unused teepot msg record
/// assert!(acks.clear_unused_record(&"teepot".to_string()).is_err()); // teepot msg record already removed
/// assert_eq!(acks.len(), 1); // confirm that teepot msg record no longer exists
///
/// assert!(acks.apply_ack(&"teepot2".to_string()).is_ok()); // lets ack second msg
/// assert!(acks.clear_unused_record(&"teepot2".to_string()).is_ok());
/// assert_eq!(acks.len(), 0);
/// ```

pub trait ApplyAck {
    fn apply_ack(&mut self, uuid: &Uuid) -> Result<(), Status>;
    fn clear_unused_record(&mut self, uuid: &Uuid) -> Result<(), Status>;
    fn add_record(&mut self, message: Message, duration: Duration);
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

    fn add_record(&mut self, message: Message, duration: Duration) {
        let uuid = message.uuid.to_owned();

        self.entry(uuid)
            .and_modify(|elem| {
                elem.increment_counter();
            })
            .or_insert(AckRecord::new(message, Instant::now(), duration));
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
