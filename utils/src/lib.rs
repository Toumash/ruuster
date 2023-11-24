use std::io;

pub fn console_input(msg: &str) -> Result<String, Box<dyn std::error::Error>> {
    println!("{}", msg);
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    Ok(buffer.trim().to_string())
}