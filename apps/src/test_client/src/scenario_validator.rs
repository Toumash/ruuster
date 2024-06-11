use clap::Parser;

use tracing::info;

mod conf_parser;
mod config_definition;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    config_file: std::path::PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to setup validator logs");

    let args = Args::parse();
    let _ = conf_parser::get_conf(&args.config_file)?;
    info!("scenario is valid");
    Ok(())
}
