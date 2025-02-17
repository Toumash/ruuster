use protos::{
    ruuster, AckMessageBulkRequest, AckRequest, BindRequest, ConsumeRequest, Empty,
    ExchangeDeclareRequest, ListExchangesResponse, ListQueuesResponse, ProduceRequest,
    QueueDeclareRequest, RemoveExchangeRequest, RemoveQueueRequest, RoutingKey, UnbindRequest,
};
use queues::queues::RuusterQueues;

use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;
use tracing::{info_span, instrument, Instrument};

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
        let request = request.into_inner();
        let queue_name = request.queue_name;

        self.0.add_queue(&queue_name)?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let request = request.into_inner();
        let (exchange_name, exchange_kind) = match request.exchange {
            Some(exchange) => (exchange.exchange_name, exchange.kind),
            None => return Err(Status::failed_precondition("bad exchange request")),
        };

        self.0.add_exchange(&exchange_name, exchange_kind.into())?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all)]
    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        let queue_names = self.0.get_queues_list()?;
        Ok(Response::new(ListQueuesResponse { queue_names }))
    }

    #[instrument(skip_all)]
    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        let exchange_names = self.0.get_exchanges_list()?;
        Ok(Response::new(ListExchangesResponse { exchange_names }))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn bind(
        &self,
        request: tonic::Request<BindRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let request: BindRequest = request.into_inner();

        // check if requested queue and exchange exists
        let _ = self.0.get_queue(&request.queue_name)?;
        let _ = self.0.get_exchange(&request.exchange_name)?;

        let metadata = request.metadata.as_ref();

        self.0
            .bind_queue_to_exchange(&request.queue_name, &request.exchange_name, metadata)?;
        Ok(Response::new(Empty {}))
    }

    type ConsumeBulkStream = ReceiverStream<Result<protos::Message, Status>>;

    /**
     * receive messages from specific queue and return it in form of asynchronous stream
     */
    async fn consume_bulk(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeBulkStream>, Status> {
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
        Ok(Response::new(result))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn consume_one(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<protos::Message>, Status> {
        let request = request.into_inner();
        let message = self
            .0
            .consume_message(&request.queue_name, request.auto_ack)?;
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
        Ok(Response::new(msg))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn produce(
        &self,
        request: tonic::Request<ProduceRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        let metadata = Some(internals::Metadata {
            created_at: None,
            dead_letter: None,
            routing_key: request.metadata.map(|m| m.routing_key.unwrap().value),
        });
        self.0
            .forward_message(request.payload, &request.exchange_name, metadata)?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all)]
    async fn ack_message(
        &self,
        _request: tonic::Request<AckRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = _request.into_inner();
        self.0.apply_message_ack(request.uuid)?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn ack_message_bulk(
        &self,
        request: tonic::Request<AckMessageBulkRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.0.apply_message_bulk_ack(&request.uuids)?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn unbind(
        &self,
        request: tonic::Request<UnbindRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.0.unbind_queue_from_exchange(
            &request.queue_name,
            &request.exchange_name,
            request.metadata.as_ref(),
        )?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn remove_queue(
        &self,
        request: tonic::Request<RemoveQueueRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.0.remove_queue(&request.queue_name)?;
        Ok(Response::new(Empty {}))
    }

    #[instrument(skip_all, fields(request=?request))]
    async fn remove_exchange(
        &self,
        request: tonic::Request<RemoveExchangeRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.0.remove_exchange(&request.exchange_name)?;
        Ok(Response::new(Empty {}))
    }
}

#[cfg(test)]
mod tests_utils;

#[cfg(test)]
mod tests;
