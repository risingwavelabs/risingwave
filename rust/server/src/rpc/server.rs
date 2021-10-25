use crate::rpc::service::exchange_service::ExchangeServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::rpc::service::task_service::TaskServiceImpl;
use crate::source::MemSourceManager;
use crate::storage::SimpleTableManager;
use crate::stream::StreamManager;
use crate::task::{GlobalTaskEnv, TaskManager};
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

pub fn rpc_serve(addr: SocketAddr) -> (JoinHandle<()>, UnboundedSender<()>) {
    let store_mgr = Arc::new(SimpleTableManager::new());
    let task_mgr = Arc::new(TaskManager::new());
    let stream_mgr = Arc::new(StreamManager::new());
    let source_mgr = Arc::new(MemSourceManager::new());
    let env = GlobalTaskEnv::new(store_mgr.clone(), source_mgr, task_mgr.clone(), addr);
    let task_srv = TaskServiceImpl::new(task_mgr.clone(), env);
    let exchange_srv = ExchangeServiceImpl::new(task_mgr);
    let stream_srv = StreamServiceImpl::new(stream_mgr, store_mgr);

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TaskServiceServer::new(task_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .serve_with_shutdown(addr, async move {
                tokio::select! {
                  _ = tokio::signal::ctrl_c() => {},
                  _ = shutdown_recv.recv() => {},
                }
            })
            .await
            .unwrap();
    });

    (join_handle, shutdown_send)
}

#[cfg(test)]
mod tests {
    use crate::rpc::server::rpc_serve;
    use crate::util::addr::get_host_port;

    #[tokio::test]
    async fn test_server_shutdown() {
        let addr = get_host_port("127.0.0.1:5688").unwrap();
        let (join_handle, shutdown_send) = rpc_serve(addr);
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
