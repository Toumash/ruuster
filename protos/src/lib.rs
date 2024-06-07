pub mod ruuster {
    include!("ruuster.rs");
}
pub use ruuster::*;

use std::fmt::Display;
use std::fmt;

impl Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.routing_key {
            Some(rk) => write!(f, "{}", rk.value),
            None => write!(f, "None")
        }
    }
}