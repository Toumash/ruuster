use std::fs::File;
use std::io::BufReader;
use std::{fmt, path::Path};

use crate::config_definition::ScenarioConfig;

#[derive(Debug)]
enum ArgsValidationError {
    InvalidPath,
}

impl fmt::Display for ArgsValidationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArgsValidationError::InvalidPath => write!(f, "Invalid path"),
        }
    }
}

impl std::error::Error for ArgsValidationError {}

fn validate_path(config_file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !config_file_path.exists() {
        return Err(Box::new(ArgsValidationError::InvalidPath));
    }
    Ok(())
}

pub fn get_conf(config_file_path: &Path) -> Result<ScenarioConfig, Box<dyn std::error::Error>> {
    validate_path(config_file_path)?;
    let file = File::open(config_file_path)?;
    let reader = BufReader::new(file);
    let scenario_config: ScenarioConfig = serde_json::from_reader(reader)?;
    Ok(scenario_config)
}
