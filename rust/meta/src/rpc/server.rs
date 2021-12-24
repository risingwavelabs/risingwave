use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::catalog_service_server::CatalogServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::epoch_service_server::EpochServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::id_generator_service_server::IdGeneratorServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::catalog::StoredCatalogManager;
use crate::cluster::StoredClusterManager;
use crate::hummock;
use crate::manager::{Config, IdGeneratorManager, MemEpochGenerator, MetaManager};
use crate::rpc::service::catalog_service::CatalogServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::epoch_service::EpochServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::id_service::IdGeneratorServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::storage::MemStore;
use crate::stream::StoredStreamMetaManager;

pub async fn rpc_serve(
    addr: SocketAddr,
    hummock_config: Option<hummock::Config>,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let meta_store_ref = Arc::new(MemStore::new());
    let config = Config::default();

    let stream_meta_manager = Arc::new(StoredStreamMetaManager::new(
        config.clone(),
        meta_store_ref.clone(),
    ));
    let id_generator_manager_ref = Arc::new(IdGeneratorManager::new(meta_store_ref.clone()).await);
    let epoch_generator_ref = Arc::new(MemEpochGenerator::new());
    let catalog_manager_ref = Arc::new(StoredCatalogManager::new(
        config.clone(),
        meta_store_ref.clone(),
        epoch_generator_ref.clone(),
    ));
    let cluster_manager = Arc::new(StoredClusterManager::new(
        meta_store_ref.clone(),
        config.clone(),
    ));
    let meta_manager = Arc::new(
        MetaManager::new(
            meta_store_ref.clone(),
            epoch_generator_ref,
            id_generator_manager_ref.clone(),
            config.clone(),
        )
        .await,
    );
    let hummock_manager = hummock::DefaultHummockManager::new(
        meta_store_ref.clone(),
        id_generator_manager_ref.clone(),
        config.clone(),
        hummock_config.unwrap_or_default(),
    )
    .await;

    let epoch_srv = EpochServiceImpl::new(meta_manager.clone());
    let heartbeat_srv = HeartbeatServiceImpl::new(catalog_manager_ref.clone());
    let catalog_srv = CatalogServiceImpl::new(catalog_manager_ref);
    let cluster_srv = ClusterServiceImpl::new(cluster_manager);
    let id_generator_srv = IdGeneratorServiceImpl::new(meta_manager);
    let stream_srv = StreamServiceImpl::new(stream_meta_manager);
    let hummock_srv = HummockServiceImpl::new(hummock_manager);

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EpochServiceServer::new(epoch_srv))
            .add_service(HeartbeatServiceServer::new(heartbeat_srv))
            .add_service(CatalogServiceServer::new(catalog_srv))
            .add_service(ClusterServiceServer::new(cluster_srv))
            .add_service(IdGeneratorServiceServer::new(id_generator_srv))
            .add_service(StreamManagerServiceServer::new(stream_srv))
            .add_service(HummockManagerServiceServer::new(hummock_srv))
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
    use risingwave_common::util::addr::get_host_port;

    use crate::rpc::server::rpc_serve;

    #[tokio::test]
    async fn test_server_shutdown() {
        let addr = get_host_port("127.0.0.1:9527").unwrap();
        let (join_handle, shutdown_send) = rpc_serve(addr, None).await;
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
