use std::str::FromStr;
use clap::Parser;

mod conf_json_def;
mod conf_parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args{
    #[arg(long)]
    config_file: std::path::PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let _ = conf_parser::get_conf(&args.config_file)?;
    println!("scenario is valid");
    Ok(())
}
