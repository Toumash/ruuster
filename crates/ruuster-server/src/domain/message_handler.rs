use crate::error::*;
use futures::Stream;
use ruuster_internals::Message;
use ruuster_router::Router;
use std::{pin::Pin, sync::Arc};
use uuid::Uuid;

use super::consumer_manager::ConsumerManager;

pub type MessageStream =
    Pin<Box<dyn Stream<Item = Result<Message, ruuster_internals::RuusterError>> + Send>>;

pub struct MessageHandler {
    router: Arc<Router>,
    consumer_manager: Arc<ConsumerManager>,
}

impl MessageHandler {
    pub fn new(router: Arc<Router>, consumer_manager: Arc<ConsumerManager>) -> Self {
        Self {
            router,
            consumer_manager,
        }
    }

    pub fn produce(&self, exchange_name: &str, msg: Message) -> Result<(), ServerError> {
        self.router
            .route(exchange_name, msg)
            .map_err(ServerError::from)
    }

    pub fn consume(
        &self,
        queue_name: &str,
        prefetch_count: u16,
    ) -> Result<MessageStream, ServerError> {
        let queue = self
            .router
            .get_queue(queue_name)
            .ok_or_else(|| ServerError::QueueNotFound(queue_name.to_string()))?;

        let consumer_id = self
            .consumer_manager
            .register_consumer(queue, prefetch_count);

        Ok(self.create_message_stream(consumer_id, queue_name))
    }

    pub fn create_message_stream(&self, consumer_id: Uuid, queue_name: &str) -> MessageStream {
        let router = Arc::clone(&self.router);
        let consumer_mgr = Arc::clone(&self.consumer_manager);
        // Without clone, lifetime issues arise - Consider using String in function signature
        let queue_name = queue_name.to_string();

        let stream = async_stream::try_stream! {
            while let Some(queue) = router.get_queue(&queue_name) {
                if !consumer_mgr.can_consume(&consumer_id) {
                    // Prefetch limit reached, wait before trying again
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }

                if let Some(msg) = queue.dequeue()? {
                    let msg_id = msg.uuid;
                    consumer_mgr.track_delivery(&consumer_id, msg_id);

                    yield msg;
                    continue;
                }

                queue.notify.notified().await;
            }
        };

        Box::pin(stream)
    }
}
