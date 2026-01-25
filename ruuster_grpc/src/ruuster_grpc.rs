use protos::{
    ruuster, AckMessageBulkRequest, AckRequest, BindRequest, ConsumeRequest, Empty,
    ExchangeDeclareRequest, ListExchangesResponse, ListQueuesResponse, NackMessageBulkRequest,
    NackRequest, ProduceRequest, QueueDeclareRequest, RemoveExchangeRequest, RemoveQueueRequest,
    RoutingKey, UnbindRequest,
};
use queues::queues::RuusterQueues;

use metrics::{counter, histogram};
use std::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;
use tracing::{info_span, instrument, Instrument};

/// Record gRPC request metrics (counter and histogram)
fn record_grpc_metrics(method: &'static str, status: &str, start: Instant) {
    let duration = start.elapsed().as_secs_f64();
    counter!("ruuster_grpc_requests_total", "method" => method, "status" => status.to_string())
        .increment(1);
    histogram!("ruuster_grpc_request_duration_seconds", "method" => method).record(duration);
}

pub struct RuusterQueuesGrpc(RuusterQueues);

impl RuusterQueuesGrpc {
    pub fn new() -> Self {
        RuusterQueuesGrpc(RuusterQueues::new())
    }
}

impl Default for RuusterQueuesGrpc {
    fn default() -> Self {
        RuusterQueuesGrpc::new()
    }
}

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueuesGrpc {
    #[instrument(skip_all, fields(request=?request))]
    async fn queue_declare(
        &self,
        request: tonic::Request<QueueDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        let queue_name = request.queue_name;

        match self.0.add_queue(&queue_name) {
            Ok(()) => {
                record_grpc_metrics("queue_declare", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("queue_declare", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        let (exchange_name, exchange_kind) = match request.exchange {
            Some(exchange) => (exchange.exchange_name, exchange.kind),
            None => {
                record_grpc_metrics("exchange_declare", "error", start);
                return Err(Status::failed_precondition("bad exchange request"));
            }
        };

        match self.0.add_exchange(&exchange_name, exchange_kind.into()) {
            Ok(()) => {
                record_grpc_metrics("exchange_declare", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("exchange_declare", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all)]
    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        let start = Instant::now();
        match self.0.get_queues_list() {
            Ok(queue_names) => {
                record_grpc_metrics("list_queues", "ok", start);
                Ok(Response::new(ListQueuesResponse { queue_names }))
            }
            Err(e) => {
                record_grpc_metrics("list_queues", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all)]
    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        let start = Instant::now();
        match self.0.get_exchanges_list() {
            Ok(exchange_names) => {
                record_grpc_metrics("list_exchanges", "ok", start);
                Ok(Response::new(ListExchangesResponse { exchange_names }))
            }
            Err(e) => {
                record_grpc_metrics("list_exchanges", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn bind(
        &self,
        request: tonic::Request<BindRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let start = Instant::now();
        let request: BindRequest = request.into_inner();

        // check if requested queue and exchange exists
        if let Err(e) = self.0.get_queue(&request.queue_name) {
            record_grpc_metrics("bind", "error", start);
            return Err(e);
        }
        if let Err(e) = self.0.get_exchange(&request.exchange_name) {
            record_grpc_metrics("bind", "error", start);
            return Err(e);
        }

        let metadata = request.metadata.as_ref();

        match self
            .0
            .bind_queue_to_exchange(&request.queue_name, &request.exchange_name, metadata)
        {
            Ok(()) => {
                record_grpc_metrics("bind", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("bind", "error", start);
                Err(e)
            }
        }
    }

    type ConsumeBulkStream = ReceiverStream<Result<protos::Message, Status>>;

    /**
     * receive messages from specific queue and return it in form of asynchronous stream
     */
    async fn consume_bulk(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeBulkStream>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        let span = info_span!(
            "consume_bulk",
            queue_name=%request.queue_name,
            auto_ack=%request.auto_ack
        );
        let queue_name = &request.queue_name;
        let auto_ack = request.auto_ack;
        let result = async move { self.0.start_consuming_task(queue_name, auto_ack).await }
            .instrument(span)
            .await;
        record_grpc_metrics("consume_bulk", "ok", start);
        Ok(Response::new(result))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn consume_one(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<protos::Message>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        match self
            .0
            .consume_message(&request.queue_name, request.auto_ack)
        {
            Ok(message) => {
                let msg = protos::Message {
                    metadata: match message.metadata {
                        Some(m) => m.routing_key.map(|rk| protos::Metadata {
                            routing_key: Some(RoutingKey { value: rk }),
                        }),
                        None => None,
                    },
                    payload: message.payload,
                    uuid: message.uuid,
                };
                record_grpc_metrics("consume_one", "ok", start);
                Ok(Response::new(msg))
            }
            Err(e) => {
                record_grpc_metrics("consume_one", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn produce(
        &self,
        request: tonic::Request<ProduceRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        let metadata = Some(internals::Metadata {
            created_at: None,
            dead_letter: None,
            routing_key: request.metadata.map(|m| m.routing_key.unwrap().value),
        });
        match self
            .0
            .forward_message(request.payload, &request.exchange_name, metadata)
        {
            Ok(_) => {
                record_grpc_metrics("produce", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("produce", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all)]
    async fn ack_message(
        &self,
        _request: tonic::Request<AckRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = _request.into_inner();
        match self.0.apply_message_ack(request.uuid) {
            Ok(()) => {
                record_grpc_metrics("ack_message", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("ack_message", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn ack_message_bulk(
        &self,
        request: tonic::Request<AckMessageBulkRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        match self.0.apply_message_bulk_ack(&request.uuids) {
            Ok(()) => {
                record_grpc_metrics("ack_message_bulk", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("ack_message_bulk", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn nack_message(
        &self,
        request: tonic::Request<NackRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        match self.0.apply_message_nack(request.uuid, request.requeue) {
            Ok(()) => {
                record_grpc_metrics("nack_message", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("nack_message", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn nack_message_bulk(
        &self,
        request: tonic::Request<NackMessageBulkRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        let nacks: Vec<(String, bool)> = request
            .nacks
            .into_iter()
            .map(|n| (n.uuid, n.requeue))
            .collect();
        match self.0.apply_message_bulk_nack(&nacks) {
            Ok(()) => {
                record_grpc_metrics("nack_message_bulk", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("nack_message_bulk", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn unbind(
        &self,
        request: tonic::Request<UnbindRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        match self.0.unbind_queue_from_exchange(
            &request.queue_name,
            &request.exchange_name,
            request.metadata.as_ref(),
        ) {
            Ok(()) => {
                record_grpc_metrics("unbind", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("unbind", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn remove_queue(
        &self,
        request: tonic::Request<RemoveQueueRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        match self.0.remove_queue(&request.queue_name) {
            Ok(()) => {
                record_grpc_metrics("remove_queue", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("remove_queue", "error", start);
                Err(e)
            }
        }
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn remove_exchange(
        &self,
        request: tonic::Request<RemoveExchangeRequest>,
    ) -> Result<Response<Empty>, Status> {
        let start = Instant::now();
        let request = request.into_inner();
        match self.0.remove_exchange(&request.exchange_name) {
            Ok(()) => {
                record_grpc_metrics("remove_exchange", "ok", start);
                Ok(Response::new(Empty {}))
            }
            Err(e) => {
                record_grpc_metrics("remove_exchange", "error", start);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests_utils;

#[cfg(test)]
mod tests;
