use crate::metadata::{Config, MemEpochGenerator, MemStore, MetaManager, StoredIdGenerator};
use crate::rpc::service::catalog_service::CatalogServiceImpl;
use crate::rpc::service::epoch_service::EpochServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::id_service::IdGeneratorServiceImpl;
use risingwave_pb::metadata::catalog_service_server::CatalogServiceServer;
use risingwave_pb::metadata::epoch_service_server::EpochServiceServer;
use risingwave_pb::metadata::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::metadata::id_generator_service_server::IdGeneratorServiceServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

pub async fn rpc_serve(addr: SocketAddr) -> (JoinHandle<()>, UnboundedSender<()>) {
    let meta_store_ref = Arc::new(MemStore::new());
    let meta_manager = Arc::new(
        MetaManager::new(
            meta_store_ref.clone(),
            Box::new(MemEpochGenerator::new()),
            Box::new(StoredIdGenerator::new(meta_store_ref).await),
            Config::default(),
        )
        .await,
    );

    let epoch_srv = EpochServiceImpl::new(meta_manager.clone());
    let heartbeat_srv = HeartbeatServiceImpl::new(meta_manager.clone());
    let catalog_srv = CatalogServiceImpl::new(meta_manager.clone());
    let id_generator_srv = IdGeneratorServiceImpl::new(meta_manager);

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EpochServiceServer::new(epoch_srv))
            .add_service(HeartbeatServiceServer::new(heartbeat_srv))
            .add_service(CatalogServiceServer::new(catalog_srv))
            .add_service(IdGeneratorServiceServer::new(id_generator_srv))
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
    use risingwave_common::util::addr::get_host_port;

    #[tokio::test]
    async fn test_server_shutdown() {
        let addr = get_host_port("127.0.0.1:9527").unwrap();
        let (join_handle, shutdown_send) = rpc_serve(addr).await;
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
