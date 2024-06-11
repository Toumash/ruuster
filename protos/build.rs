use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "./defs/ruuster.proto";
    tonic_build::configure()
        .build_server(true)
        .out_dir("./src")
        .compile(&[proto_file], &["."])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    let status = Command::new("protoc")
        .args([
            "--proto_path=./defs",
            "--include_imports",
            "--descriptor_set_out=defs/ruuster_descriptor.bin",
            "./defs/ruuster.proto",
        ])
        .status()
        .expect("failed to execute protoc");

    assert!(status.success(), "protoc encounterd an error");

    Ok(())
}
