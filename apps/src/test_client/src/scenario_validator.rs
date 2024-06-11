use std::{env, path::PathBuf, str::FromStr};

mod conf_parser;
mod conf_json_def;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let path_buf = PathBuf::from_str(&args[1])?;
    let _ = conf_parser::get_conf(&path_buf)?;
    println!("scenario is vaild");
    Ok(())
}