use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::catalog_service_server::CatalogServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::epoch_service_server::EpochServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::intercept::MetricsMiddlewareLayer;
use super::metrics::MetaMetrics;
use super::service::notification_service::NotificationServiceImpl;
use crate::barrier::BarrierManager;
use crate::cluster::StoredClusterManager;
use crate::dashboard::DashboardService;
use crate::hummock;
use crate::manager::{MemEpochGenerator, MetaSrvEnv, NotificationManager, StoredCatalogManager};
use crate::rpc::service::catalog_service::CatalogServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::epoch_service::EpochServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::storage::MemStore;
use crate::stream::{FragmentManager, StreamManager};

pub enum MetaStoreBackend {
    Mem,
}

pub async fn rpc_serve(
    addr: SocketAddr,
    prometheus_addr: Option<SocketAddr>,
    dashboard_addr: Option<SocketAddr>,
    meta_store_backend: MetaStoreBackend,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let meta_store_ref = match meta_store_backend {
        MetaStoreBackend::Mem => Arc::new(MemStore::default()),
    };
    let epoch_generator_ref = Arc::new(MemEpochGenerator::new());
    let env =
        MetaSrvEnv::<MemStore>::new(meta_store_ref.clone(), epoch_generator_ref.clone()).await;

    let fragment_manager = Arc::new(FragmentManager::new(meta_store_ref.clone()).await.unwrap());
    let hummock_manager = Arc::new(hummock::HummockManager::new(env.clone()).await.unwrap());
    let notification_manager = Arc::new(NotificationManager::new());
    let cluster_manager = Arc::new(
        StoredClusterManager::new(
            env.clone(),
            Some(hummock_manager.clone()),
            notification_manager.clone(),
        )
        .await
        .unwrap(),
    );

    if let Some(dashboard_addr) = dashboard_addr {
        let dashboard_service = DashboardService {
            dashboard_addr,
            cluster_manager: cluster_manager.clone(),
            fragment_manager: fragment_manager.clone(),
            meta_store_ref: env.meta_store_ref(),
            has_test_data: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        tokio::spawn(dashboard_service.serve()); // TODO: join dashboard service back to local
                                                 // thread
    }

    let barrier_manager_ref = Arc::new(BarrierManager::new(
        env.clone(),
        cluster_manager.clone(),
        fragment_manager.clone(),
        epoch_generator_ref.clone(),
    ));
    {
        let barrier_manager_ref = barrier_manager_ref.clone();
        // TODO: join barrier service back to local thread
        tokio::spawn(async move { barrier_manager_ref.run().await.unwrap() });
    }

    let stream_manager_ref = Arc::new(
        StreamManager::new(
            env.clone(),
            fragment_manager.clone(),
            barrier_manager_ref,
            cluster_manager.clone(),
        )
        .await
        .unwrap(),
    );
    let catalog_manager_ref = Arc::new(
        StoredCatalogManager::new(meta_store_ref.clone())
            .await
            .unwrap(),
    );

    let epoch_srv = EpochServiceImpl::new(epoch_generator_ref.clone());
    let heartbeat_srv = HeartbeatServiceImpl::new();
    let catalog_srv = CatalogServiceImpl::<MemStore>::new(env.clone(), catalog_manager_ref);
    let cluster_srv = ClusterServiceImpl::<MemStore>::new(cluster_manager.clone());
    let stream_srv = StreamServiceImpl::<MemStore>::new(
        stream_manager_ref,
        fragment_manager.clone(),
        cluster_manager,
        env,
    );
    let hummock_srv = HummockServiceImpl::new(hummock_manager);
    let notification_srv = NotificationServiceImpl::new(notification_manager);
    let meta_metrics = Arc::new(MetaMetrics::new());

    if let Some(prometheus_addr) = prometheus_addr {
        meta_metrics.boot_metrics_service(prometheus_addr);
    }

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(MetricsMiddlewareLayer::new(meta_metrics.clone()))
            .add_service(EpochServiceServer::new(epoch_srv))
            .add_service(HeartbeatServiceServer::new(heartbeat_srv))
            .add_service(CatalogServiceServer::new(catalog_srv))
            .add_service(ClusterServiceServer::new(cluster_srv))
            .add_service(StreamManagerServiceServer::new(stream_srv))
            .add_service(HummockManagerServiceServer::new(hummock_srv))
            .add_service(NotificationServiceServer::new(notification_srv))
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

#[cfg(test)]
mod tests {
    use crate::rpc::server::{rpc_serve, MetaStoreBackend};

    #[tokio::test]
    async fn test_server_shutdown() {
        let addr = "127.0.0.1:9527".parse().unwrap();
        let (join_handle, shutdown_send) = rpc_serve(addr, None, None, MetaStoreBackend::Mem).await;
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
