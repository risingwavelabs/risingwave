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
use crate::storage::MetaStoreRef;
use crate::stream::FragmentManager;

#[derive(Clone)]
pub struct DashboardService {
    pub dashboard_addr: SocketAddr,
    pub cluster_manager: Arc<StoredClusterManager>,
    pub fragment_manager: Arc<FragmentManager>,

    // TODO: replace with catalog manager.
    pub meta_store_ref: MetaStoreRef,
    pub has_test_data: Arc<AtomicBool>,
}

pub type Service = Arc<DashboardService>;

mod handlers {
    use axum::Json;
    use risingwave_pb::common::WorkerNode;
    use risingwave_pb::meta::{ActorLocation, Table};
    use risingwave_pb::stream_plan::StreamActor;
    use serde_json::json;

    use super::*;

    pub struct DashboardError(anyhow::Error);
    pub type Result<T> = std::result::Result<T, DashboardError>;
    type TableId = i32;
    type TableActors = (TableId, Vec<StreamActor>);

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
        let result = srv.cluster_manager.list_worker_node(
            WorkerType::from_i32(ty)
                .ok_or_else(|| anyhow!("invalid worker type"))
                .map_err(err)?,
        );
        Ok(result.into())
    }

    pub async fn list_materialized_views(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<(TableId, Table)>>> {
        use crate::model::MetadataModel;

        let materialized_views = Table::list(&srv.meta_store_ref)
            .await
            .map_err(err)?
            .iter()
            .filter(|t| t.is_materialized_view)
            .map(|mv| (mv.table_ref_id.as_ref().unwrap().table_id, mv.clone()))
            .collect::<Vec<_>>();
        Ok(Json(materialized_views))
    }

    pub async fn list_actors(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<ActorLocation>>> {
        use risingwave_pb::common::WorkerType;

        let node_actors = srv.fragment_manager.load_all_node_actors().map_err(err)?;
        let nodes = srv
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode);
        let actors = nodes
            .iter()
            .map(|node| ActorLocation {
                node: Some(node.clone()),
                actors: node_actors.get(&node.id).cloned().unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        Ok(Json(actors))
    }

    pub async fn list_table_fragments(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<TableActors>>> {
        let table_fragments = srv
            .fragment_manager
            .list_table_fragments()
            .map_err(err)?
            .iter()
            .map(|f| (f.table_id().table_id(), f.actors()))
            .collect::<Vec<_>>();

        Ok(Json(table_fragments))
    }
}

impl DashboardService {
    pub async fn serve(self) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);
        let app = Router::new()
            .route("/api/clusters/:ty", get(list_clusters))
            .route("/api/actors", get(list_actors))
            .route("/api/fragments", get(list_table_fragments))
            .route("/api/materialized_views", get(list_materialized_views))
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
