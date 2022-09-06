// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, get_service};
use axum::Router;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::cors::{self, CorsLayer};
use tower_http::services::ServeDir;

use crate::manager::{ClusterManagerRef, FragmentManagerRef};
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct DashboardService<S: MetaStore> {
    pub dashboard_addr: SocketAddr,
    pub cluster_manager: ClusterManagerRef<S>,
    pub fragment_manager: FragmentManagerRef<S>,

    // TODO: replace with catalog manager.
    pub meta_store: Arc<S>,
}

pub type Service<S> = Arc<DashboardService<S>>;

mod handlers {
    use axum::Json;
    use itertools::Itertools;
    use risingwave_pb::catalog::{Source, Table};
    use risingwave_pb::common::WorkerNode;
    use risingwave_pb::meta::{ActorLocation, TableFragments as ProstTableFragments};
    use risingwave_pb::stream_plan::StreamActor;
    use serde_json::json;

    use super::*;
    use crate::model::TableFragments;

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

    pub async fn list_clusters<S: MetaStore>(
        Path(ty): Path<i32>,
        Extension(srv): Extension<Service<S>>,
    ) -> Result<Json<Vec<WorkerNode>>> {
        use risingwave_pb::common::WorkerType;
        let result = srv
            .cluster_manager
            .list_worker_node(
                WorkerType::from_i32(ty)
                    .ok_or_else(|| anyhow!("invalid worker type"))
                    .map_err(err)?,
                None,
            )
            .await;
        Ok(result.into())
    }

    pub async fn list_materialized_views<S: MetaStore>(
        Extension(srv): Extension<Service<S>>,
    ) -> Result<Json<Vec<Table>>> {
        use crate::model::MetadataModel;

        let materialized_views = Table::list(&*srv.meta_store).await.map_err(err)?;
        Ok(Json(materialized_views))
    }

    pub async fn list_sources<S: MetaStore>(
        Extension(srv): Extension<Service<S>>,
    ) -> Result<Json<Vec<Source>>> {
        use crate::model::MetadataModel;

        let sources = Source::list(&*srv.meta_store).await.map_err(err)?;
        Ok(Json(sources))
    }

    pub async fn list_actors<S: MetaStore>(
        Extension(srv): Extension<Service<S>>,
    ) -> Result<Json<Vec<ActorLocation>>> {
        use risingwave_pb::common::WorkerType;

        let node_actors = srv.fragment_manager.all_node_actors(true).await;
        let nodes = srv
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, None)
            .await;
        let actors = nodes
            .iter()
            .map(|node| ActorLocation {
                node: Some(node.clone()),
                actors: node_actors.get(&node.id).cloned().unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        Ok(Json(actors))
    }

    pub async fn list_table_fragments<S: MetaStore>(
        Extension(srv): Extension<Service<S>>,
    ) -> Result<Json<Vec<TableActors>>> {
        let table_fragments = srv
            .fragment_manager
            .list_table_fragments()
            .await
            .map_err(err)?
            .iter()
            .map(|f| (f.table_id().table_id() as i32, f.actors()))
            .collect::<Vec<_>>();

        Ok(Json(table_fragments))
    }

    pub async fn list_fragments<S: MetaStore>(
        Extension(srv): Extension<Service<S>>,
    ) -> Result<Json<Vec<ProstTableFragments>>> {
        use crate::model::MetadataModel;

        let table_fragments = TableFragments::list(&*srv.meta_store)
            .await
            .map_err(err)?
            .into_iter()
            .map(|x| x.to_protobuf())
            .collect_vec();
        Ok(Json(table_fragments))
    }
}

impl<S> DashboardService<S>
where
    S: MetaStore,
{
    pub async fn serve(self, ui_path: Option<String>) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let api_router = Router::new()
            .route("/clusters/:ty", get(list_clusters::<S>))
            .route("/actors", get(list_actors::<S>))
            .route("/fragments", get(list_table_fragments::<S>))
            .route("/fragments2", get(list_fragments::<S>))
            .route("/materialized_views", get(list_materialized_views::<S>))
            .route("/sources", get(list_sources::<S>))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(
                // TODO: allow wildcard CORS is dangerous! Should remove this in production.
                CorsLayer::new().allow_origin(cors::Any).allow_methods(vec![
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::DELETE,
                ]),
            );

        let app = if let Some(ui_path) = ui_path {
            let static_file_router = Router::new().nest(
                "/",
                get_service(ServeDir::new(ui_path)).handle_error(
                    |error: std::io::Error| async move {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", error),
                        )
                    },
                ),
            );
            Router::new()
                .fallback(static_file_router)
                .nest("/api", api_router)
        } else {
            Router::new()
                .route(
                    "/",
                    get(|| async { Html::from(include_str!("index.html")) }),
                )
                .nest("/api", api_router)
        };

        axum::Server::bind(&srv.dashboard_addr)
            .serve(app.into_make_service())
            .await
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }
}
