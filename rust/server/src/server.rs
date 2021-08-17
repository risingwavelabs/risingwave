use crate::service::exchange_service::ExchangeServiceImpl;
use crate::service::task_service::TaskServiceImpl;
use crate::storage::MemStorageManager;
use crate::task::{GlobalTaskEnv, TaskManager};
use grpcio::{Environment, Result, ServerBuilder, ShutdownFuture};
use risingwave_proto::task_service_grpc::{create_exchange_service, create_task_service};
use std::sync::{Arc, Mutex};

pub struct Server {
    rpc_srv: grpcio::Server,
}

impl Server {
    pub fn new() -> Result<Server> {
        let store_mgr = Arc::new(MemStorageManager::new());
        let env = GlobalTaskEnv::new(store_mgr);

        let task_mgr = Arc::new(Mutex::new(TaskManager::new(env)));
        let task_srv = TaskServiceImpl::new(task_mgr.clone());
        let exchange_srv = ExchangeServiceImpl::new(task_mgr);
        let grpc_srv = ServerBuilder::new(Arc::new(Environment::new(1)))
            .bind("0.0.0.0", 5688)
            .register_service(create_task_service(task_srv))
            .register_service(create_exchange_service(exchange_srv))
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
