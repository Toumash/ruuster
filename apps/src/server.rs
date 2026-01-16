use opentelemetry::{trace::TraceError, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use protos::ruuster_server::RuusterServer;
use ruuster_grpc::RuusterQueuesGrpc;
use std::fs;
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{filter, Layer, Registry};

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:50051";
const SERVER_ADDR_ENV: &str = "RUUSTER_SERVER_ADDR";

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
                "ruuster-tracer",
            )])),
        )
        .install_batch(runtime::Tokio)
}

fn resolve_server_addr() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let addr = std::env::var(SERVER_ADDR_ENV).unwrap_or_else(|_| DEFAULT_SERVER_ADDR.to_string());
    Ok(addr.parse()?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracer = init_tracer().expect("Failed to initialize tracer.");
    let filter_layer = filter::Targets::new().with_targets([
        ("ruuster_grpc", LevelFilter::INFO),
        ("queues", LevelFilter::INFO),
        ("exchanges", LevelFilter::INFO),
    ]);
    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default()
        .with(telemetry)
        .with(filter_layer)
        .with(stdout_log.with_filter(filter::LevelFilter::INFO));
    tracing::subscriber::set_global_default(subscriber)?;

    let addr = resolve_server_addr()?;
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
    {
        let span = tracing::info_span!("app_start");
        let _enter = span.enter();
        info!("starting server on address: {}", &addr);
    }

    Server::builder()
        .add_service(RuusterServer::new(ruuster_queue_service))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
