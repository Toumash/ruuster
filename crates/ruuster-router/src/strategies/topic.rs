use super::RoutingStrategy;
use dashmap::DashSet;
use ruuster_core::Queue;
use ruuster_internals::{Message, RuusterError};
use std::sync::Arc;

/// Topic routing strategy with pattern matching support.
///
/// Routing keys and binding patterns are dot-separated words (e.g., "stock.usd.nyse").
/// Patterns support wildcards:
/// - `*` matches exactly one word
/// - `#` matches zero or more words
///
/// Examples:
/// - Pattern `stock.*.nyse` matches `stock.usd.nyse` but not `stock.nyse` or `stock.usd.nasdaq`
/// - Pattern `stock.#` matches `stock.usd.nyse`, `stock.usd`, and `stock`
/// - Pattern `#.nyse` matches `stock.usd.nyse`, `usd.nyse`, and `nyse`
pub struct TopicStrategy;

impl TopicStrategy {
    /// Check if a routing key matches a binding pattern
    fn matches_pattern(routing_key: &str, pattern: &str) -> bool {
        let key_words: Vec<&str> = routing_key.split('.').collect();
        let pattern_words: Vec<&str> = pattern.split('.').collect();

        Self::matches_recursive(&key_words, &pattern_words, 0, 0)
    }

    fn matches_recursive(
        key_words: &[&str],
        pattern_words: &[&str],
        key_idx: usize,
        pattern_idx: usize,
    ) -> bool {
        // Both exhausted - match!
        if key_idx >= key_words.len() && pattern_idx >= pattern_words.len() {
            return true;
        }

        // Pattern exhausted but key has more words - no match
        if pattern_idx >= pattern_words.len() {
            return false;
        }

        match pattern_words[pattern_idx] {
            // '#' matches zero or more words
            "#" => {
                // Try matching zero words (skip '#')
                if Self::matches_recursive(key_words, pattern_words, key_idx, pattern_idx + 1) {
                    return true;
                }

                // Try matching one or more words
                if key_idx < key_words.len() {
                    return Self::matches_recursive(key_words, pattern_words, key_idx + 1, pattern_idx);
                }

                false
            }
            // '*' matches exactly one word
            "*" => {
                if key_idx < key_words.len() {
                    Self::matches_recursive(key_words, pattern_words, key_idx + 1, pattern_idx + 1)
                } else {
                    false
                }
            }
            // Literal word - must match exactly
            word => {
                if key_idx < key_words.len() && key_words[key_idx] == word {
                    Self::matches_recursive(key_words, pattern_words, key_idx + 1, pattern_idx + 1)
                } else {
                    false
                }
            }
        }
    }
}

impl RoutingStrategy for TopicStrategy {
    fn route(&self, msg: Message, bindings: &DashSet<Arc<Queue>>) -> Result<(), RuusterError> {
        let routing_key = msg
            .routing_key
            .as_deref()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| {
                RuusterError::InvalidMetadata(
                    "Topic routing requires a non-empty routing key".to_string(),
                )
            })?;

        let mut matched = false;

        // Check each bound queue's name as a pattern
        for queue in bindings.iter() {
            if Self::matches_pattern(routing_key, &queue.name) {
                queue.enqueue(msg.clone())?;
                matched = true;
            }
        }

        if matched {
            Ok(())
        } else {
            Err(RuusterError::InvalidMetadata(format!(
                "No queue pattern matched routing key: {}",
                routing_key
            )))
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
    fn test_exact_match() {
        assert!(TopicStrategy::matches_pattern("stock.usd", "stock.usd"));
        assert!(!TopicStrategy::matches_pattern("stock.usd", "stock.eur"));
    }

    #[test]
    fn test_star_wildcard() {
        // '*' matches exactly one word
        assert!(TopicStrategy::matches_pattern("stock.usd", "stock.*"));
        assert!(TopicStrategy::matches_pattern("stock.usd", "*.usd"));
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse", "stock.*.nyse"));
        
        // Must match exactly one word
        assert!(!TopicStrategy::matches_pattern("stock", "stock.*"));
        assert!(!TopicStrategy::matches_pattern("stock.usd.nyse", "stock.*"));
    }

    #[test]
    fn test_hash_wildcard() {
        // '#' matches zero or more words
        assert!(TopicStrategy::matches_pattern("stock", "stock.#"));
        assert!(TopicStrategy::matches_pattern("stock.usd", "stock.#"));
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse", "stock.#"));
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse.latest", "stock.#"));
        
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse", "#.nyse"));
        assert!(TopicStrategy::matches_pattern("nyse", "#.nyse"));
        
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse", "#"));
        assert!(TopicStrategy::matches_pattern("anything", "#"));
    }

    #[test]
    fn test_combined_wildcards() {
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse", "*.*.nyse"));
        assert!(TopicStrategy::matches_pattern("stock.usd.nyse", "stock.#.nyse"));
        assert!(TopicStrategy::matches_pattern("stock.usd.extra.nyse", "stock.#.nyse"));
        
        assert!(!TopicStrategy::matches_pattern("stock.nyse", "*.*.nyse"));
        assert!(TopicStrategy::matches_pattern("stock.nyse", "stock.#.nyse"));
    }

    #[test]
    fn test_topic_routing_single_match() {
        let strategy = TopicStrategy;
        let bindings = DashSet::new();

        let nyse_q = Arc::new(Queue::new("*.*.nyse".into(), 10));
        let all_stock_q = Arc::new(Queue::new("stock.#".into(), 10));
        let eur_q = Arc::new(Queue::new("*.eur.*".into(), 10));

        bindings.insert(Arc::clone(&nyse_q));
        bindings.insert(Arc::clone(&all_stock_q));
        bindings.insert(Arc::clone(&eur_q));

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: Some("stock.usd.nyse".into()),
            payload: Arc::new(vec![]),
            metadata: None,
        };

        strategy.route(msg, &bindings).unwrap();

        // Should match both *.*.nyse and stock.#
        assert_eq!(nyse_q.len(), 1);
        assert_eq!(all_stock_q.len(), 1);
        assert_eq!(eur_q.len(), 0);
    }

    #[test]
    fn test_topic_routing_multiple_matches() {
        let strategy = TopicStrategy;
        let bindings = DashSet::new();

        let all_q = Arc::new(Queue::new("#".into(), 10));
        let stock_q = Arc::new(Queue::new("stock.*".into(), 10));
        let usd_q = Arc::new(Queue::new("*.usd".into(), 10));

        bindings.insert(Arc::clone(&all_q));
        bindings.insert(Arc::clone(&stock_q));
        bindings.insert(Arc::clone(&usd_q));

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: Some("stock.usd".into()),
            payload: Arc::new(vec![]),
            metadata: None,
        };

        strategy.route(msg, &bindings).unwrap();

        // Should match all three patterns
        assert_eq!(all_q.len(), 1);
        assert_eq!(stock_q.len(), 1);
        assert_eq!(usd_q.len(), 1);
    }

    #[test]
    fn test_topic_routing_no_match() {
        let strategy = TopicStrategy;
        let bindings = DashSet::new();

        let nyse_q = Arc::new(Queue::new("*.nyse".into(), 10));
        bindings.insert(Arc::clone(&nyse_q));

        let msg = Message {
            uuid: Uuid::new_v4(),
            routing_key: Some("stock.nasdaq".into()),
            payload: Arc::new(vec![]),
            metadata: None,
        };

        let result = strategy.route(msg, &bindings);
        assert!(result.is_err());
        assert_eq!(nyse_q.len(), 0);
    }

    #[test]
    fn test_empty_routing_key() {
        let strategy = TopicStrategy;
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
}
