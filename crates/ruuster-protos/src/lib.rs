pub mod v1 {
    // The standard generated code
    tonic::include_proto!("ruuster_v1");
    tonic::include_proto!("topology_v1");

    // The binary data required for gRPC Reflection
    // This looks for the file we named in build.rs
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/ruuster_descriptor.bin"));
}
