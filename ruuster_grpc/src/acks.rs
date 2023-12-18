use protos::Message;


struct AckRecord {
    message: Message,
    counter: isize,
    timestamp: std::time::Instant,
    deadline: std::time::Instant
}

impl AckRecord {
    fn new() -> Self {
        todo!()
    }

    fn is_deadline_exceeded(&self) -> bool {
        false
    }

    fn apply_ack(&self) {

    }

    fn get_message(&self) -> &Message {
        &self.message
    }

    fn get_timestamp(&self) -> &std::time::Instant {
        &self.timestamp
    }

    fn get_deadline(&self) -> &std::time::Instant {
        &self.deadline
    }
}