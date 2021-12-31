use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use log::info;
use risingwave_common::error::Result;
use warp::Filter;

use crate::cluster::{StoredClusterManager, WorkerNodeMetaManager};

#[derive(Clone)]
pub struct DashboardService {
    pub dashboard_addr: SocketAddr,
    pub cluster_manager: Arc<StoredClusterManager>,
    pub has_test_data: Arc<AtomicBool>,
}

pub type Service = Arc<DashboardService>;
use std::convert::Infallible;

mod handlers {
    use itertools::Itertools;
    use serde::Serialize;

    use super::*;

    #[derive(Serialize)]
    pub struct WorkerNode {
        pub id: u32,
        pub host: String,
        pub port: i32,
    }

    pub async fn list_clusters(
        ty: i32,
        srv: Service,
    ) -> std::result::Result<impl warp::Reply, Infallible> {
        srv.add_test_data().await.unwrap();

        use risingwave_pb::meta::ClusterType;
        let result = srv
            .cluster_manager
            .list_worker_node(ClusterType::from_i32(ty).unwrap()) // TODO: error handling
            .await
            .unwrap_or_else(|_| vec![])
            .into_iter()
            .map(|node| WorkerNode {
                id: node.get_id(),
                host: node.get_host().get_host().to_owned(),
                port: node.get_host().get_port(),
            })
            .collect_vec(); // TODO: handle error
        Ok(warp::reply::json(&result))
    }
}

mod filters {
    use super::*;

    pub fn with_service(
        srv: Service,
    ) -> impl Filter<Extract = (Service,), Error = Infallible> + Clone {
        warp::any().map(move || srv.clone())
    }

    pub fn clusters_list(
        srv: Service,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("clusters" / i32)
            .and(warp::get())
            .and(with_service(srv))
            .and_then(handlers::list_clusters)
    }

    pub fn clusters(
        srv: Service,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        clusters_list(srv)
    }
}

impl DashboardService {
    pub async fn serve(self) -> Result<()> {
        let srv = Arc::new(self);

        info!("starting dashboard service at {:?}", srv.dashboard_addr);
        let api = warp::path("api").and(filters::clusters(srv.clone()));

        let index = warp::get().and(warp::path::end()).map(|| {
            warp::http::Response::builder()
                .header("content-type", "text/html; charset=utf-8")
                .body(std::str::from_utf8(include_bytes!("index.html")).unwrap())
        });
        warp::serve(index.or(api)).run(srv.dashboard_addr).await;
        Ok(())
    }

    pub async fn add_test_data(self: &Arc<Self>) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.has_test_data.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.has_test_data.store(true, Ordering::SeqCst);

        // TODO: remove adding test data
        use risingwave_pb::common::HostAddress;
        use risingwave_pb::meta::ClusterType;

        let hosts = (0..3)
            .map(|e| HostAddress {
                host: "127.0.0.1".to_string(),
                port: (8888 + e) as i32,
            })
            .collect::<Vec<_>>();

        for host in &hosts[..2] {
            self.cluster_manager
                .add_worker_node(host.clone(), ClusterType::ComputeNode)
                .await?;
        }

        for host in &hosts[2..] {
            self.cluster_manager
                .add_worker_node(host.clone(), ClusterType::Frontend)
                .await?;
        }

        Ok(())
    }
}
