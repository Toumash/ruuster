pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
pub type RpcRequest<T> = tonic::Request<T>;

pub fn parse_uuid(bytes: &[u8]) -> Result<uuid::Uuid, tonic::Status> {
    uuid::Uuid::from_slice(bytes)
        .map_err(|_| tonic::Status::invalid_argument("Invalid UUID format"))
}

#[macro_export]
macro_rules! RpcOk {
    () => {
        Ok(tonic::Response::new(()))
    };
    ($value:expr) => {
        Ok(tonic::Response::new($value))
    };
}
