use crate::service::exchange_service::ExchangeServiceImpl;
use crate::service::stream_service::StreamServiceImpl;
use crate::service::task_service::TaskServiceImpl;
use crate::source::MemSourceManager;
use crate::storage::MemStorageManager;
use crate::stream::StreamManager;
use crate::task::{GlobalTaskEnv, TaskManager};
use grpcio::{Environment, Result, ServerBuilder, ShutdownFuture};
use risingwave_proto::stream_service_grpc::create_stream_service;
use risingwave_proto::task_service_grpc::{create_exchange_service, create_task_service};
use std::net::SocketAddr;
use std::sync::Arc;

pub struct Server {
    rpc_srv: grpcio::Server,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Result<Server> {
        let store_mgr = Arc::new(MemStorageManager::new());

        let task_mgr = Arc::new(TaskManager::new());
        let stream_mgr = Arc::new(StreamManager::new());
        let source_mgr = Arc::new(MemSourceManager::new());
        let env = GlobalTaskEnv::new(store_mgr, source_mgr, task_mgr.clone(), addr);
        let task_srv = TaskServiceImpl::new(task_mgr.clone(), env);
        let exchange_srv = ExchangeServiceImpl::new(task_mgr);
        let stream_srv = StreamServiceImpl::new(stream_mgr);
        let grpc_srv = ServerBuilder::new(Arc::new(Environment::new(2)))
            .bind(addr.ip().to_string().as_str(), addr.port())
            .register_service(create_task_service(task_srv))
            .register_service(create_exchange_service(exchange_srv))
            .register_service(create_stream_service(stream_srv))
            .build()?;
        Ok(Server { rpc_srv: grpc_srv })
    }

    pub fn start(&mut self) {
        self.rpc_srv.start();
    }

    pub fn shutdown(&mut self) -> ShutdownFuture {
        self.rpc_srv.shutdown()
    }
}
