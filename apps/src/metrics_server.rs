//! Prometheus metrics HTTP server for Ruuster.
//!
//! Exposes a `/metrics` endpoint for Prometheus scraping.

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Initialize the Prometheus metrics recorder and return the handle for rendering metrics.
pub fn init_metrics_recorder() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus metrics recorder")
}

/// Handle incoming HTTP requests - only `/metrics` endpoint is supported.
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    handle: PrometheusHandle,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match req.uri().path() {
        "/metrics" => {
            let metrics = handle.render();
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Full::new(Bytes::from(metrics)))
                .unwrap()
        }
        "/health" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("OK")))
            .unwrap(),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap(),
    };
    Ok(response)
}

/// Start the metrics HTTP server on the given address.
///
/// This function spawns a background task that serves the `/metrics` endpoint.
pub async fn start_metrics_server(
    addr: SocketAddr,
    handle: PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    info!("Metrics server listening on http://{}/metrics", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let handle = handle.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let handle = handle.clone();
                handle_request(req, handle)
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                error!("Error serving metrics connection: {:?}", err);
            }
        });
    }
}
