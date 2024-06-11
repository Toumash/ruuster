use std::io;

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

pub fn random_float(begin: f64, end: f64) -> f64 {
    let mut rng = thread_rng();
    rng.gen_range(begin..=end)
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
