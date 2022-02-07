use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use risingwave_common::error::ErrorCode;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::cluster::StoredClusterManager;
use crate::stream::{StoredStreamMetaManager, StreamMetaManager};

#[derive(Clone)]
pub struct DashboardService {
    pub dashboard_addr: SocketAddr,
    pub cluster_manager: Arc<StoredClusterManager>,
    pub stream_meta_manager: Arc<StoredStreamMetaManager>,
    pub has_test_data: Arc<AtomicBool>,
}

pub type Service = Arc<DashboardService>;

mod handlers {
    use axum::Json;
    use risingwave_pb::common::WorkerNode;
    use risingwave_pb::meta::ActorLocation;
    use serde_json::json;

    use super::*;

    pub struct DashboardError(anyhow::Error);
    pub type Result<T> = std::result::Result<T, DashboardError>;

    fn err(err: impl Into<anyhow::Error>) -> DashboardError {
        DashboardError(err.into())
    }

    impl IntoResponse for DashboardError {
        fn into_response(self) -> axum::response::Response {
            let mut resp = Json(json!({
              "error": format!("{}", self.0),
              "info":  format!("{:?}", self.0),
            }))
            .into_response();
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }

    pub async fn list_clusters(
        Path(ty): Path<i32>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<WorkerNode>>> {
        srv.add_test_data().await.map_err(err)?;

        use risingwave_pb::common::WorkerType;
        let result = srv
            .cluster_manager
            .list_worker_node(
                WorkerType::from_i32(ty)
                    .ok_or_else(|| anyhow!("invalid worker type"))
                    .map_err(err)?,
            ) // TODO: error handling
            .map_err(err)?;
        Ok(result.into())
    }

    pub async fn list_actors(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<ActorLocation>>> {
        let actors = srv
            .stream_meta_manager
            .load_all_actors()
            .await
            .map_err(err)?;
        Ok(Json(actors))
    }
}

impl DashboardService {
    pub async fn serve(self) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);
        let app = Router::new()
            .route("/api/clusters/:ty", get(list_clusters))
            .route("/api/actors", get(list_actors))
            .route(
                "/",
                get(|| async { Html::from(include_str!("index.html")) }),
            )
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(
                // TODO: allow wildcard CORS is dangerous! Should remove this in production.
                CorsLayer::new()
                    .allow_origin(cors::any())
                    .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE]),
            );

        axum::Server::bind(&srv.dashboard_addr)
            .serve(app.into_make_service())
            .await
            .map_err(|err| ErrorCode::MetaError(err.to_string()))?;
        Ok(())
    }

    pub async fn add_test_data(self: &Arc<Self>) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.has_test_data.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.has_test_data.store(true, Ordering::SeqCst);

        // TODO: remove adding test data
        use risingwave_pb::common::{HostAddress, WorkerType};

        // TODO: remove adding frontend register when frontend implement register.
        self.cluster_manager
            .add_worker_node(
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 4567,
                },
                WorkerType::Frontend,
            )
            .await?;

        Ok(())
    }
}
