mod acks;
pub mod queues;

use protos::{
    ruuster, AckRequest, BindQueueToExchangeRequest, ConsumeRequest, Empty, ExchangeDeclareRequest,
    ListBindingsRequest, ListBindingsResponse, ListExchangesResponse, ListQueuesResponse, Message,
    ProduceRequest, QueueDeclareRequest,
};
use queues::RuusterQueues;

use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueues{
    async fn queue_declare(
        &self,
        request: tonic::Request<QueueDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let request = request.into_inner();
        let queue_name = request.queue_name;

        self.add_queue(&queue_name)?;
        Ok(Response::new(Empty {}))
    }

    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        let request = request.into_inner();
        let (exchange_name, exchange_kind) = match request.exchange {
            Some(exchange) => (exchange.exchange_name, exchange.kind),
            None => return Err(Status::failed_precondition("bad exchange request")),
        };

        self.add_exchange(&exchange_name, exchange_kind.into())?;
        Ok(Response::new(Empty {}))
    }

    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        let queue_names = self.get_queues_list()?;
        Ok(Response::new(ListQueuesResponse { queue_names }))
    }

    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        let exchange_names = self.get_exchanges_list()?;
        Ok(Response::new(ListExchangesResponse { exchange_names }))
    }

    async fn bind_queue_to_exchange(
        &self,
        request: tonic::Request<BindQueueToExchangeRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let request = request.into_inner();

        let _ = self.get_queue(&request.queue_name)?;
        let _ = self.get_exchange(&request.exchange_name)?;

        self.bind_queue_to_exchange(&request.header, &request.queue_name, &request.exchange_name)?;
        Ok(Response::new(Empty {}))
    }

    //NOTICE(msaff): this is not an only option, if it will not be good enough we can always change it to smoething more... low-level ;)
    type ConsumeStream = ReceiverStream<Result<Message, Status>>;

    /**
     * this request will receive messages from specific queue and return it in form of asynchronous stream
     */
    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let queue_name = &request.into_inner().queue_name;
        let async_receiver = self.start_consuming_task(queue_name).await;
        Ok(Response::new(async_receiver))
    }

    async fn consume_one(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Message>, Status> {
        let request = request.into_inner();
        let message = self.consume_message(&request.queue_name)?;
        Ok(Response::new(message))
    }

    /**
     * for now let's have a simple producer that will push message into exchange requested in message header
     */
    async fn produce(
        &self,
        request: tonic::Request<ProduceRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.forward_message(&request.payload, &request.exchange_name)?;
        Ok(Response::new(Empty {}))
    }

    async fn ack_message(
        &self,
        _request: tonic::Request<AckRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.apply_message_ack(request.uuid)?;
        Ok(Response::new(Empty {}))
    }

    async fn list_bindings(
        &self,
        _request: tonic::Request<ListBindingsRequest>,
    ) -> Result<Response<ListBindingsResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests_utils;

#[cfg(test)]
mod tests;
