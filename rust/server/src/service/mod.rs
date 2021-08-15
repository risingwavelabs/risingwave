pub(crate) mod exchange_service;
pub(crate) mod task_service;

pub(crate) type ExchangeWriteStream =
    grpcio::ServerStreamingSink<risingwave_proto::task_service::TaskData>;
