use std::fs;

use protos::ruuster_server::RuusterServer;

use ruuster_grpc::RuusterQueuesGrpc;
use tonic::transport::Server;

use opentelemetry::{
    trace::TraceError,
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use tracing::{info, span};

// use tracing::{error, span};
// use tracing_subscriber::layer::SubscriberExt;
// use tracing_subscriber::Registry;
const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: &str = "50051";



fn init_tracer() -> Result<opentelemetry_sdk::trace::Tracer, TraceError> {
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317"),
        )
        .with_trace_config(
            sdktrace::config().with_resource(Resource::new(vec![KeyValue::new(
                SERVICE_NAME,
                "tracing-jaeger",
            )])),
        )
        .install_batch(runtime::Tokio)
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    
    let tracer = init_tracer().expect("Failed to initialize tracer.");
    // Create a new OpenTelemetry trace pipeline that prints to stdout


    // Create a tracing layer with the configured tracer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // // Use the tracing subscriber `Registry`, or any other subscriber
    // // that impls `LookupSpan`
    let subscriber = Registry::default().with(telemetry);


    let addr = format!("{}:{}", SERVER_IP, SERVER_PORT).parse().unwrap();
    let ruuster_queue_service = RuusterQueuesGrpc::new();
    let current_dir = std::env::current_dir()?;
    let ruuster_descriptor_path = current_dir
        .join("protos")
        .join("defs")
        .join("ruuster_descriptor.bin");

    let ruuster_descriptor_content = fs::read(ruuster_descriptor_path)?;

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(&ruuster_descriptor_content)
        .build()?;

    // Trace executed code
    tracing::subscriber::with_default(subscriber, || {
        // Spans will be sent to the configured OpenTelemetry exporter
        let root = span!(tracing::Level::TRACE, "app_start", work_units = 2);
        let _enter = root.enter();

        info!("starting server on address: {}", &addr);
    });

    Server::builder()
        .add_service(RuusterServer::new(ruuster_queue_service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

        Ok(())
}
