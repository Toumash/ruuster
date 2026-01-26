use super::RoutingStrategy;
use dashmap::DashSet;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::collections::HashMap;
use std::sync::Arc;

/// Header routing strategy with key-value matching support.
///
/// Routes messages based on metadata headers rather than routing keys.
/// Each queue binding specifies required headers and match mode:
/// - `all`: All specified headers must match (AND logic)
/// - `any`: At least one header must match (OR logic)
///
/// Queue names should follow the format: `mode:key1=value1,key2=value2`
/// Examples:
/// - `all:format=pdf,type=report` - Matches if format=pdf AND type=report
/// - `any:priority=high,priority=urgent` - Matches if priority=high OR priority=urgent
///
/// Message headers are stored in `MessageMetadata.headers` as a `HashMap<String, String>`.
///
/// Common use cases:
/// - Content-based routing: `format=json`, `encoding=utf8`
/// - Priority queues: `priority=high`, `priority=low`
/// - Multi-criteria filtering: `region=us,type=order,status=pending`
pub struct HeaderStrategy;

#[derive(Debug, Clone, PartialEq)]
enum MatchMode {
    All, // AND - all headers must match
    Any, // OR - at least one header must match
}

#[derive(Debug, Clone)]
struct BindingPattern {
    mode: MatchMode,
    headers: HashMap<String, String>,
}

impl HeaderStrategy {
    /// Parse a queue name into a binding pattern
    /// Format: "mode:key1=value1,key2=value2"
    /// Example: "all:format=pdf,type=report"
    fn parse_binding_pattern(queue_name: &str) -> Option<BindingPattern> {
        let parts: Vec<&str> = queue_name.splitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }

        let mode = match parts[0] {
            "all" => MatchMode::All,
            "any" => MatchMode::Any,
            _ => return None,
        };

        let mut headers = HashMap::new();
        for pair in parts[1].split(',') {
            let kv: Vec<&str> = pair.splitn(2, '=').collect();
            if kv.len() == 2 {
                headers.insert(kv[0].trim().to_string(), kv[1].trim().to_string());
            }
        }

        if headers.is_empty() {
            return None;
        }

        Some(BindingPattern { mode, headers })
    }

    /// Extract headers from message metadata
    fn extract_headers(msg: &Message) -> HashMap<String, String> {
        msg.metadata
            .as_ref()
            .map(|m| m.headers.clone())
            .unwrap_or_default()
    }

    /// Check if message headers match the binding pattern
    fn matches_pattern(msg_headers: &HashMap<String, String>, pattern: &BindingPattern) -> bool {
        match pattern.mode {
            MatchMode::All => {
                // All required headers must match
                pattern
                    .headers
                    .iter()
                    .all(|(k, v)| msg_headers.get(k).map(|mv| mv == v).unwrap_or(false))
            }
            MatchMode::Any => {
                // At least one header must match
                pattern
                    .headers
                    .iter()
                    .any(|(k, v)| msg_headers.get(k).map(|mv| mv == v).unwrap_or(false))
            }
        }
    }
}

impl RoutingStrategy for HeaderStrategy {
    fn route(&self, msg: Message, bindings: &DashSet<Arc<Queue>>) -> Result<(), RuusterError> {
        let msg_headers = Self::extract_headers(&msg);

        if msg_headers.is_empty() {
            return Err(RuusterError::InvalidMetadata(
                "Header routing requires message headers in metadata".to_string(),
            ));
        }

        let mut matched = false;

        for queue in bindings.iter() {
            if let Some(pattern) = Self::parse_binding_pattern(&queue.name)
                && Self::matches_pattern(&msg_headers, &pattern)
            {
                queue.enqueue(msg.clone())?;
                matched = true;
            }
        }

        if matched {
            Ok(())
        } else {
            Err(RuusterError::InvalidMetadata(
                "No queue pattern matched message headers".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashSet;
    use ruuster_core::Queue;
    use ruuster_internals::Message;
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn test_parse_binding_pattern_all() {
        let pattern = HeaderStrategy::parse_binding_pattern("all:format=pdf,type=report").unwrap();
        assert_eq!(pattern.mode, MatchMode::All);
        assert_eq!(pattern.headers.get("format").unwrap(), "pdf");
        assert_eq!(pattern.headers.get("type").unwrap(), "report");
    }

    #[test]
    fn test_parse_binding_pattern_any() {
        let pattern =
            HeaderStrategy::parse_binding_pattern("any:priority=high,priority=urgent").unwrap();
        assert_eq!(pattern.mode, MatchMode::Any);
        assert!(pattern.headers.contains_key("priority"));
    }

    #[test]
    fn test_parse_binding_pattern_invalid() {
        assert!(HeaderStrategy::parse_binding_pattern("invalid").is_none());
        assert!(HeaderStrategy::parse_binding_pattern("all:").is_none());
        assert!(HeaderStrategy::parse_binding_pattern("unknown:key=value").is_none());
    }

    #[test]
    fn test_extract_headers() {
        let mut headers = HashMap::new();
        headers.insert("format".to_string(), "pdf".to_string());
        headers.insert("type".to_string(), "report".to_string());
        headers.insert("priority".to_string(), "high".to_string());

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: Some(ruuster_internals::MessageMetadata {
                client_time: None,
                arrival_time: None,
                headers: headers.clone(),
            }),
        };

        let extracted = HeaderStrategy::extract_headers(&msg);
        assert_eq!(extracted.get("format").unwrap(), "pdf");
        assert_eq!(extracted.get("type").unwrap(), "report");
        assert_eq!(extracted.get("priority").unwrap(), "high");
    }

    #[test]
    fn test_match_all_success() {
        let mut msg_headers = HashMap::new();
        msg_headers.insert("format".to_string(), "pdf".to_string());
        msg_headers.insert("type".to_string(), "report".to_string());
        msg_headers.insert("priority".to_string(), "high".to_string());

        let pattern = BindingPattern {
            mode: MatchMode::All,
            headers: {
                let mut h = HashMap::new();
                h.insert("format".to_string(), "pdf".to_string());
                h.insert("type".to_string(), "report".to_string());
                h
            },
        };

        assert!(HeaderStrategy::matches_pattern(&msg_headers, &pattern));
    }

    #[test]
    fn test_match_all_failure() {
        let mut msg_headers = HashMap::new();
        msg_headers.insert("format".to_string(), "pdf".to_string());
        msg_headers.insert("type".to_string(), "invoice".to_string());

        let pattern = BindingPattern {
            mode: MatchMode::All,
            headers: {
                let mut h = HashMap::new();
                h.insert("format".to_string(), "pdf".to_string());
                h.insert("type".to_string(), "report".to_string());
                h
            },
        };

        assert!(!HeaderStrategy::matches_pattern(&msg_headers, &pattern));
    }

    #[test]
    fn test_match_any_success() {
        let mut msg_headers = HashMap::new();
        msg_headers.insert("priority".to_string(), "high".to_string());

        let pattern = BindingPattern {
            mode: MatchMode::Any,
            headers: {
                let mut h = HashMap::new();
                h.insert("priority".to_string(), "high".to_string());
                h.insert("status".to_string(), "urgent".to_string());
                h
            },
        };

        assert!(HeaderStrategy::matches_pattern(&msg_headers, &pattern));
    }

    #[test]
    fn test_header_routing_all_mode() {
        let strategy = HeaderStrategy;
        let bindings = DashSet::new();

        let pdf_reports = Arc::new(Queue::new("all:format=pdf,type=report".into(), 10));
        let high_priority = Arc::new(Queue::new("all:priority=high".into(), 10));

        bindings.insert(Arc::clone(&pdf_reports));
        bindings.insert(Arc::clone(&high_priority));

        let mut headers = HashMap::new();
        headers.insert("format".to_string(), "pdf".to_string());
        headers.insert("type".to_string(), "report".to_string());
        headers.insert("priority".to_string(), "high".to_string());

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: Some(ruuster_internals::MessageMetadata {
                client_time: None,
                arrival_time: None,
                headers,
            }),
        };

        strategy.route(msg, &bindings).unwrap();

        assert_eq!(pdf_reports.len(), 1);
        assert_eq!(high_priority.len(), 1);
    }

    #[test]
    fn test_header_routing_any_mode() {
        let strategy = HeaderStrategy;
        let bindings = DashSet::new();

        let urgent = Arc::new(Queue::new("any:priority=high,priority=urgent".into(), 10));
        bindings.insert(Arc::clone(&urgent));

        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), "urgent".to_string());
        headers.insert("type".to_string(), "alert".to_string());

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: Some(ruuster_internals::MessageMetadata {
                client_time: None,
                arrival_time: None,
                headers,
            }),
        };

        strategy.route(msg, &bindings).unwrap();
        assert_eq!(urgent.len(), 1);
    }

    #[test]
    fn test_header_routing_no_match() {
        let strategy = HeaderStrategy;
        let bindings = DashSet::new();

        let pdf_queue = Arc::new(Queue::new("all:format=pdf".into(), 10));
        bindings.insert(Arc::clone(&pdf_queue));

        let mut headers = HashMap::new();
        headers.insert("format".to_string(), "json".to_string());

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: Some(ruuster_internals::MessageMetadata {
                client_time: None,
                arrival_time: None,
                headers,
            }),
        };

        let result = strategy.route(msg, &bindings);
        assert!(result.is_err());
        assert_eq!(pdf_queue.len(), 0);
    }

    #[test]
    fn test_header_routing_empty_headers() {
        let strategy = HeaderStrategy;
        let bindings = DashSet::new();

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: None,
        };

        let result = strategy.route(msg, &bindings);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_headers_empty() {
        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: None,
        };

        let headers = HeaderStrategy::extract_headers(&msg);
        assert!(headers.is_empty());
    }

    #[test]
    fn test_extract_headers_with_multiple_values() {
        let mut headers = HashMap::new();
        headers.insert("key".to_string(), "value=with=equals".to_string());
        headers.insert("normal".to_string(), "value".to_string());

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: Some(ruuster_internals::MessageMetadata {
                client_time: None,
                arrival_time: None,
                headers: headers.clone(),
            }),
        };

        let extracted = HeaderStrategy::extract_headers(&msg);
        assert_eq!(extracted.get("key").unwrap(), "value=with=equals");
        assert_eq!(extracted.get("normal").unwrap(), "value");
    }

    #[test]
    fn test_header_routing_with_empty_values() {
        let strategy = HeaderStrategy;
        let bindings = DashSet::new();

        let queue = Arc::new(Queue::new("all:empty=".into(), 10));
        bindings.insert(Arc::clone(&queue));

        let mut headers = HashMap::new();
        headers.insert("empty".to_string(), "".to_string());

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: None,
            payload: Arc::new(vec![]),
            metadata: Some(ruuster_internals::MessageMetadata {
                client_time: None,
                arrival_time: None,
                headers,
            }),
        };

        strategy.route(msg, &bindings).unwrap();
        assert_eq!(queue.len(), 1);
    }
}
