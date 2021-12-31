use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::catalog_service_server::CatalogServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::epoch_service_server::EpochServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::id_generator_service_server::IdGeneratorServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::catalog::StoredCatalogManager;
use crate::cluster::StoredClusterManager;
use crate::dashboard::DashboardService;
use crate::hummock;
use crate::manager::{Config, MemEpochGenerator, MetaSrvEnv};
use crate::rpc::service::catalog_service::CatalogServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::epoch_service::EpochServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::id_service::IdGeneratorServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::storage::MemStore;
use crate::stream::{DefaultStreamManager, StoredStreamMetaManager};

pub async fn rpc_serve_with_listener(
    listener: TcpListener,
    dashboard_addr: Option<SocketAddr>,
    hummock_config: Option<hummock::Config>,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let config = Arc::new(Config::default());
    let meta_store_ref = Arc::new(MemStore::new());
    let epoch_generator_ref = Arc::new(MemEpochGenerator::new());
    let env = MetaSrvEnv::new(config, meta_store_ref, epoch_generator_ref).await;

    let stream_meta_manager = Arc::new(StoredStreamMetaManager::new(env.clone()));
    let catalog_manager_ref = Arc::new(StoredCatalogManager::new(env.clone()));
    let cluster_manager = Arc::new(StoredClusterManager::new(env.clone()));
    let (hummock_manager, _) =
        hummock::DefaultHummockManager::new(env.clone(), hummock_config.unwrap_or_default())
            .await
            .unwrap();
    let stream_manager_ref = Arc::new(DefaultStreamManager::new(
        stream_meta_manager.clone(),
        cluster_manager.clone(),
    ));

    if let Some(dashboard_addr) = dashboard_addr {
        let dashboard_service = DashboardService {
            dashboard_addr,
            cluster_manager: cluster_manager.clone(),
            has_test_data: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        tokio::spawn(dashboard_service.serve()); // TODO: join dashboard service back to local
                                                 // thread
    }

    let epoch_srv = EpochServiceImpl::new(env.clone());
    let heartbeat_srv = HeartbeatServiceImpl::new(catalog_manager_ref.clone());
    let catalog_srv = CatalogServiceImpl::new(catalog_manager_ref);
    let cluster_srv = ClusterServiceImpl::new(cluster_manager.clone());
    let id_generator_srv = IdGeneratorServiceImpl::new(env.clone());
    let stream_srv = StreamServiceImpl::new(
        stream_meta_manager,
        stream_manager_ref,
        cluster_manager,
        env,
    );
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
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    tokio::select! {
                      _ = tokio::signal::ctrl_c() => {},
                      _ = shutdown_recv.recv() => {},
                    }
                },
            )
            .await
            .unwrap();
    });

    (join_handle, shutdown_send)
}

pub async fn rpc_serve(
    addr: SocketAddr,
    dashboard_addr: Option<SocketAddr>,
    hummock_config: Option<hummock::Config>,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let (join_handle, shutdown) =
        rpc_serve_with_listener(listener, dashboard_addr, hummock_config).await;
    (join_handle, shutdown)
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::addr::get_host_port;

    use crate::rpc::server::rpc_serve;

    #[tokio::test]
    async fn test_server_shutdown() {
        let addr = get_host_port("127.0.0.1:9527").unwrap();
        let (join_handle, shutdown_send) = rpc_serve(addr, None, None).await;
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
