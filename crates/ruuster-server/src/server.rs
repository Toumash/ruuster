use ruuster_acks::AckManager;
use ruuster_router::Router;
use std::sync::Arc;

use crate::domain::consumer_manager::ConsumerManager;
use crate::domain::message_handler::MessageHandler;
use crate::domain::topology_manager::TopologyManager;

/// Central broker server state
pub struct RuusterServer {
    // Routing layer
    pub router: Arc<Router>,

    // Consumer management
    pub consumer_manager: Arc<ConsumerManager>,

    // Acknowledgment tracking
    pub ack_manager: Arc<AckManager>,

    // Message produce/consume orchestration
    pub message_handler: Arc<MessageHandler>,

    // Topology management (exchanges/queues)
    pub topology_manager: Arc<TopologyManager>,
}

impl RuusterServer {
    pub fn new(router: Arc<Router>) -> Self {
        let consumer_manager = Arc::new(ConsumerManager::new());
        let ack_manager = Arc::new(AckManager::new());
        let topology_manager = Arc::new(TopologyManager::new(Arc::clone(&router)));
        let message_handler = Arc::new(MessageHandler::new(
            Arc::clone(&router),
            Arc::clone(&consumer_manager),
        ));

        Self {
            router,
            consumer_manager,
            ack_manager,
            message_handler,
            topology_manager,
        }
    }
}
