use crate::service::exchange_service::ExchangeServiceImpl;
use crate::service::stream_service::StreamServiceImpl;
use crate::service::task_service::TaskServiceImpl;
use crate::source::MemSourceManager;
use crate::storage::MemStorageManager;
use crate::stream::StreamManager;
use crate::task::{GlobalTaskEnv, TaskManager};
use grpcio::{ChannelBuilder, Environment, Result, ServerBuilder, ShutdownFuture};
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
        let env = GlobalTaskEnv::new(store_mgr.clone(), source_mgr, task_mgr.clone(), addr);
        let task_srv = TaskServiceImpl::new(task_mgr.clone(), env);
        let exchange_srv = ExchangeServiceImpl::new(task_mgr);
        let stream_srv = StreamServiceImpl::new(stream_mgr, store_mgr);
        let grpc_env = Arc::new(Environment::new(2));
        let channel_args = ChannelBuilder::new(grpc_env.clone())
            .reuse_port(false) // Detect conflicted port.
            .build_args();
        let grpc_srv = ServerBuilder::new(grpc_env)
            .channel_args(channel_args)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::addr::get_host_port;

    #[test]
    fn test_server_port_conflict() {
        let addr = get_host_port("127.0.0.1:5687").unwrap();

        let mut srv = Server::new(addr).unwrap();
        // Will thrown error "Address already in use"
        assert!(Server::new(addr).is_err());
        futures::executor::block_on(srv.shutdown()).unwrap();
    }
}
