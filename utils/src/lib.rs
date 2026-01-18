use std::{io, time::Duration};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{distributions::Alphanumeric, thread_rng, Rng};

/// Convinient function for grabing input from terminal.
///
/// # Examples
///
/// ```
/// let input = utils::console_input("elo wale wiadro");
/// ```
pub fn console_input(msg: &str) -> Result<String, Box<dyn std::error::Error>> {
    println!("{}", msg);
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    Ok(buffer.trim().to_string())
}

/// Generates a random alphanumeric string of a given length.
///
/// # Examples
///
/// ```
/// let random_string = utils::generate_random_string(10);
/// assert_eq!(random_string.len(), 10);
/// ```
pub fn generate_random_string(length: usize) -> String {
    let rng = thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Utility function for getting current time as milliseconds since UNIX_EPOCH.
////
/// # Examples
///
/// ```
/// let current_time_ms = utils::current_time_duration();
/// ```
pub fn current_time_duration() -> Duration {
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).expect("Time went backwards")
}
