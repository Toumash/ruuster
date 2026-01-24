use std::sync::Arc;
use ruuster_router::Router;

pub struct RuusterServer {
    pub router: Arc<Router>,
}

impl RuusterServer {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }
}