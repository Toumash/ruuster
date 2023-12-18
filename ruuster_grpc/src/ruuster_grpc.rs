mod acks;
mod queues;


use protos::{ruuster, ListBindingsRequest, ListBindingsResponse, Empty, Message, BindQueueToExchangeRequest, ListExchangesResponse, ListQueuesResponse, QueueDeclareRequest, ExchangeDeclareRequest, ConsumeRequest, ProduceRequest, AckRequest};
use queues::RuusterQueues;

use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;
use tonic::Status;

impl Default for RuusterQueues {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl ruuster::ruuster_server::Ruuster for RuusterQueues {
    async fn queue_declare(
        &self,
        request: tonic::Request<QueueDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        
        Ok(Response::new(Empty {}))
    }

    async fn exchange_declare(
        &self,
        request: tonic::Request<ExchangeDeclareRequest>,
    ) -> Result<tonic::Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn list_queues(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListQueuesResponse>, tonic::Status> {
        todo!()
    }

    async fn list_exchanges(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ListExchangesResponse>, tonic::Status> {
        todo!()
    }

    async fn bind_queue_to_exchange(
        &self,
        request: tonic::Request<BindQueueToExchangeRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        todo!()
    }

    //NOTICE(msaff): this is not an only option, if it will not be good enough we can always change it to smoething more... lov-level ;)
    type ConsumeStream = ReceiverStream<Result<Message, Status>>;

    /**
     * this request will receive messages from specific queue and return it in form of asynchronous stream
     */
    async fn consume(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        todo!()
    }

    async fn consume_one(
        &self,
        request: tonic::Request<ConsumeRequest>,
    ) -> Result<Response<Message>, Status> {
        todo!()
    }

    /**
     * for now let's have a simple producer that will push message into exchange requested in message header
     */
    async fn produce(
        &self,
        request: tonic::Request<ProduceRequest>,
    ) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn ack_message(
        &self,
        request: tonic::Request<AckRequest>,
    ) -> Result<Response<Empty>, Status> {
        // log::trace!("ack_message: {:#?}", request);
        // let requested_uuid = &request.into_inner().uuid;
        // let acks_read = self.acks.read().unwrap();
        // let ack_flag_option = acks_read.get(requested_uuid);

        // if ack_flag_option.is_none() {
        //     let msg = format!(
        //         "message: {} requested for ack doesn't exist",
        //         requested_uuid
        //     );
        //     log::error!("{}", msg);
        //     return Err(Status::not_found(msg));
        // }

        // let ack_flag = ack_flag_option.unwrap();
        // ack_flag.0.notify_one();

        Ok(Response::new(Empty {}))
    }

    async fn list_bindings(
        &self,
        request: tonic::Request<ListBindingsRequest>,
    ) -> Result<Response<ListBindingsResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests_utils;

#[cfg(test)]
mod tests;
