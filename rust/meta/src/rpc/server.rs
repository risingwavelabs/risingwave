use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use etcd_client::{Client as EtcdClient, ConnectOptions};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::catalog_service_server::CatalogServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::epoch_service_server::EpochServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinHandle;

use super::intercept::MetricsMiddlewareLayer;
use super::service::notification_service::NotificationServiceImpl;
use crate::barrier::BarrierManager;
use crate::cluster::StoredClusterManager;
use crate::dashboard::DashboardService;
use crate::hummock;
use crate::manager::{MemEpochGenerator, MetaSrvEnv, NotificationManager, StoredCatalogManager};
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::service::catalog_service::CatalogServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::epoch_service::EpochServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::storage::{EtcdMetaStore, MemStore, MetaStore};
use crate::stream::{FragmentManager, StreamManager};

#[derive(Debug)]
pub enum MetaStoreBackend {
    Etcd { endpoints: Vec<String> },
    Mem,
}

pub async fn rpc_serve(
    addr: SocketAddr,
    prometheus_addr: Option<SocketAddr>,
    dashboard_addr: Option<SocketAddr>,
    meta_store_backend: MetaStoreBackend,
    max_heartbeat_interval: Duration,
) -> Result<(JoinHandle<()>, UnboundedSender<()>)> {
    Ok(match meta_store_backend {
        MetaStoreBackend::Etcd { endpoints } => {
            let client = EtcdClient::connect(
                endpoints,
                Some(
                    ConnectOptions::default()
                        .with_keep_alive(Duration::from_secs(3), Duration::from_secs(5)),
                ),
            )
            .await
            .map_err(|e| RwError::from(InternalError(format!("failed to connect etcd {}", e))))?;
            let meta_store_ref = Arc::new(EtcdMetaStore::new(client));
            rpc_serve_with_store(
                addr,
                prometheus_addr,
                dashboard_addr,
                meta_store_ref,
                max_heartbeat_interval,
            )
            .await
        }
        MetaStoreBackend::Mem => {
            let meta_store_ref = Arc::new(MemStore::default());
            rpc_serve_with_store(
                addr,
                prometheus_addr,
                dashboard_addr,
                meta_store_ref,
                max_heartbeat_interval,
            )
            .await
        }
    })
}

pub async fn rpc_serve_with_store<S: MetaStore>(
    addr: SocketAddr,
    prometheus_addr: Option<SocketAddr>,
    dashboard_addr: Option<SocketAddr>,
    meta_store_ref: Arc<S>,
    max_heartbeat_interval: Duration,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    let epoch_generator_ref = Arc::new(MemEpochGenerator::new());
    let env = MetaSrvEnv::<S>::new(meta_store_ref.clone(), epoch_generator_ref.clone()).await;

    let fragment_manager = Arc::new(FragmentManager::new(meta_store_ref.clone()).await.unwrap());
    let meta_metrics = Arc::new(MetaMetrics::new());
    let hummock_manager = Arc::new(
        hummock::HummockManager::new(env.clone(), meta_metrics.clone())
            .await
            .unwrap(),
    );
    let compactor_manager = Arc::new(hummock::CompactorManager::new());

    #[cfg(not(test))]
    let (delete_worker_sender, delete_worker_receiver) = mpsc::unbounded_channel();
    #[cfg(test)]
    let (_, delete_worker_receiver) = mpsc::unbounded_channel();

    let notification_manager = Arc::new(NotificationManager::new(delete_worker_receiver));
    let cluster_manager = Arc::new(
        StoredClusterManager::new(
            env.clone(),
            Some(hummock_manager.clone()),
            notification_manager.clone(),
            max_heartbeat_interval,
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
        // TODO: join dashboard service back to local thread.
        tokio::spawn(dashboard_service.serve());
    }
    let barrier_manager_ref = Arc::new(BarrierManager::new(
        env.clone(),
        cluster_manager.clone(),
        fragment_manager.clone(),
        epoch_generator_ref.clone(),
        hummock_manager.clone(),
        meta_metrics.clone(),
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
        StoredCatalogManager::new(meta_store_ref.clone(), notification_manager.clone())
            .await
            .unwrap(),
    );

    let epoch_srv = EpochServiceImpl::new(epoch_generator_ref.clone());
    let heartbeat_srv = HeartbeatServiceImpl::new(cluster_manager.clone());
    let catalog_srv = CatalogServiceImpl::<S>::new(env.clone(), catalog_manager_ref);
    let cluster_srv = ClusterServiceImpl::<S>::new(cluster_manager.clone());
    let stream_srv = StreamServiceImpl::<S>::new(
        stream_manager_ref,
        fragment_manager.clone(),
        cluster_manager.clone(),
        env,
    );
    let hummock_srv = HummockServiceImpl::new(hummock_manager.clone(), compactor_manager.clone());
    let notification_srv = NotificationServiceImpl::new(notification_manager);

    if let Some(prometheus_addr) = prometheus_addr {
        meta_metrics.boot_metrics_service(prometheus_addr);
    }

    let sub_tasks: Vec<(JoinHandle<()>, UnboundedSender<()>)> = vec![
        hummock::start_compaction_trigger(hummock_manager, compactor_manager),
        #[cfg(not(test))]
        StoredClusterManager::start_heartbeat_checker(
            cluster_manager.clone(),
            Duration::from_secs(1),
            delete_worker_sender,
        )
        .await,
    ];

    let (shutdown_send, mut shutdown_recv) = mpsc::unbounded_channel();
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
                        _ = shutdown_recv.recv() => {
                            for (join_handle, shutdown_sender) in sub_tasks {
                                if shutdown_sender.send(()).is_ok() {
                                    if let Err(err) = join_handle.await {
                                        tracing::warn!("shutdown err: {}", err);
                                    }
                                }
                            }
                        },
                    }
                },
            )
            .await
            .unwrap();
    });

    (join_handle, shutdown_send)
}
