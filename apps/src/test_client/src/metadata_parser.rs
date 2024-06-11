use tracing::error;
use protos::RoutingKey;

pub fn parse_metadata(config_metadata: &Option<String>) -> Option<protos::Metadata> {
    match config_metadata {
        None => None,
        Some(meta) => {
            let meta_parsed: crate::config_definition::Metadata = match serde_json::from_str(meta) {
                Ok(value) => value,
                Err(e) => {
                    error!(error=%e, "error while parsing metadata");
                    return None;
                }
            };
            Some(protos::Metadata {
                routing_key: Some(RoutingKey{ value: meta_parsed.routing_key.clone()})
            })
        },
    }
}