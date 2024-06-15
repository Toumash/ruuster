use std::fmt::Display;

use clap::Parser;
use conf_json_def::ScenarioConfig;
use std::process::{Child, Command};
use thiserror::Error;

mod conf_json_def;
mod conf_parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    server_addr: String,

    #[arg(long)]
    config_file: std::path::PathBuf,

    #[arg(long)]
    builder_bin: std::path::PathBuf,

    #[arg(long)]
    consumer_bin: std::path::PathBuf,

    #[arg(long)]
    producer_bin: std::path::PathBuf,
}

#[derive(Debug, Error)]
enum ProcessManagerError {
    BinaryNotExist,
}

impl Display for ProcessManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ProcessManagerError::BinaryNotExist => write!(f, "Invalid path to binary"),
        }
    }
}

struct ProcessManager {
    args: Args,
    config: ScenarioConfig,
}

impl ProcessManager {
    fn prepare_builder_command(&self) -> Result<String, Box<dyn std::error::Error>> {
        let builder_path = self.args.builder_bin.clone();
        if !builder_path.exists() {
            return Err(ProcessManagerError::BinaryNotExist.into());
        }

        Ok(format!(
            "{} --server-addr {} --config-file {}",
            builder_path.display(),
            self.args.server_addr.clone(),
            self.args.config_file.display()
        ))
    }

    fn prepare_consumer_command(
        &self,
        source: String,
        consuming_method: String,
        ack_method: String,
        min_delay_ms: i32,
        max_delay_ms: i32,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let consumer_path = self.args.consumer_bin.clone();
        if !consumer_path.exists() {
            return Err(ProcessManagerError::BinaryNotExist.into());
        }

        Ok(format!(
            "{} --server-addr {} --source {} --consuming-method {} --ack-method {} --min-delay-ms {} --max-delay-ms {}",
            consumer_path.display(),
            self.args.server_addr.clone(),
            source,
            consuming_method,
            ack_method,
            min_delay_ms,
            max_delay_ms
        ))
    }

    fn prepare_producer_command(
        &self,
        destination: String,
        messages_produced: i32,
        message_payload_bytes: i32,
        delay_ms: i32,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let producer_path = self.args.producer_bin.clone();
        if !producer_path.exists() {
            return Err(ProcessManagerError::BinaryNotExist.into());
        }

        Ok(format!(
            "{} --server-addr {} --destination {} --messages-produced {} --message-payload-bytes {} --delay-ms {}",
            producer_path.display(),
            self.args.server_addr.clone(),
            destination,
            messages_produced,
            message_payload_bytes,
            delay_ms
        ))
    }

    fn new(args: Args) -> Result<Self, Box<dyn std::error::Error>> {
        let config = conf_parser::get_conf(args.config_file.as_path())?;
        Ok(ProcessManager { args, config })
    }

    /*
       1. run scenario builder process,
       2. wait for it to finish
       3. run consumers processes
       4. run producers processes
       5. wait for consumers and producers to finish
    */
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let command_str = self.prepare_builder_command()?;
        let parts: Vec<&str> = command_str.split_whitespace().collect();
        let mut child = Command::new(parts[0])
            .args(&parts[1..])
            .spawn()
            .expect("failed to execute process");

        child.wait()?;

        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let process_manager = ProcessManager::new(args)?;
    process_manager.run()?;

    Ok(())
}
