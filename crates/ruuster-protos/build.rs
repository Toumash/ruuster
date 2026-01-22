use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);

    let proto_file = root.join("defs").join("ruuster.proto");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("ruuster_descriptor.bin");

    tonic_prost_build::configure()
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&[proto_file], &[root.join("defs")])?;

    Ok(())
}
