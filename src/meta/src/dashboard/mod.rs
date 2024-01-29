// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod prometheus;
mod proxy;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path as FilePath;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::body::{boxed, Body};
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, get_service};
use axum::Router;
use hyper::Request;
use parking_lot::Mutex;
use risingwave_rpc_client::ComputeClientPool;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::cors::{self, CorsLayer};
use tower_http::services::ServeDir;

use crate::manager::diagnose::DiagnoseCommandRef;
use crate::manager::MetadataManager;

#[derive(Clone)]
pub struct DashboardService {
    pub dashboard_addr: SocketAddr,
    pub prometheus_client: Option<prometheus_http_query::Client>,
    pub prometheus_selector: String,
    pub metadata_manager: MetadataManager,
    pub compute_clients: ComputeClientPool,
    pub ui_path: Option<String>,
    pub diagnose_command: Option<DiagnoseCommandRef>,
}

pub type Service = Arc<DashboardService>;

pub(super) mod handlers {
    use anyhow::Context;
    use axum::Json;
    use itertools::Itertools;
    use risingwave_common::bail;
    use risingwave_common_heap_profiling::COLLAPSED_SUFFIX;
    use risingwave_pb::catalog::table::TableType;
    use risingwave_pb::catalog::{Sink, Source, Table, View};
    use risingwave_pb::common::{WorkerNode, WorkerType};
    use risingwave_pb::meta::{ActorLocation, PbTableFragments};
    use risingwave_pb::monitor_service::{
        HeapProfilingResponse, ListHeapProfilingResponse, StackTraceResponse,
    };
    use serde_json::json;
    use thiserror_ext::AsReport;

    use super::*;
    use crate::manager::WorkerId;
    use crate::model::MetadataModel;

    pub struct DashboardError(anyhow::Error);
    pub type Result<T> = std::result::Result<T, DashboardError>;

    pub fn err(err: impl Into<anyhow::Error>) -> DashboardError {
        DashboardError(err.into())
    }

    impl From<anyhow::Error> for DashboardError {
        fn from(value: anyhow::Error) -> Self {
            DashboardError(value)
        }
    }

    impl IntoResponse for DashboardError {
        fn into_response(self) -> axum::response::Response {
            let mut resp = Json(json!({
                "error": self.0.to_report_string(),
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
        let worker_type = WorkerType::try_from(ty)
            .map_err(|_| anyhow!("invalid worker type"))
            .map_err(err)?;
        let mut result = srv
            .metadata_manager
            .list_worker_node(Some(worker_type), None)
            .await
            .map_err(err)?;
        result.sort_unstable_by_key(|n| n.id);
        Ok(result.into())
    }

    async fn list_table_catalogs_inner(
        metadata_manager: &MetadataManager,
        table_type: TableType,
    ) -> Result<Json<Vec<Table>>> {
        let tables = match metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_tables_by_type(table_type).await,
            MetadataManager::V2(mgr) => mgr
                .catalog_controller
                .list_tables_by_type(table_type.into())
                .await
                .map_err(err)?,
        };

        Ok(Json(tables))
    }

    pub async fn list_materialized_views(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.metadata_manager, TableType::MaterializedView).await
    }

    pub async fn list_tables(Extension(srv): Extension<Service>) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.metadata_manager, TableType::Table).await
    }

    pub async fn list_indexes(Extension(srv): Extension<Service>) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.metadata_manager, TableType::Index).await
    }

    pub async fn list_internal_tables(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<Table>>> {
        list_table_catalogs_inner(&srv.metadata_manager, TableType::Internal).await
    }

    pub async fn list_sources(Extension(srv): Extension<Service>) -> Result<Json<Vec<Source>>> {
        let sources = srv.metadata_manager.list_sources().await.map_err(err)?;

        Ok(Json(sources))
    }

    pub async fn list_sinks(Extension(srv): Extension<Service>) -> Result<Json<Vec<Sink>>> {
        let sinks = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_sinks().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_sinks().await.map_err(err)?,
        };

        Ok(Json(sinks))
    }

    pub async fn list_views(Extension(srv): Extension<Service>) -> Result<Json<Vec<View>>> {
        let views = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_views().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_views().await.map_err(err)?,
        };

        Ok(Json(views))
    }

    pub async fn list_actors(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<ActorLocation>>> {
        let mut node_actors = srv
            .metadata_manager
            .all_node_actors(true)
            .await
            .map_err(err)?;
        let nodes = srv
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .map_err(err)?;
        let actors = nodes
            .into_iter()
            .map(|node| ActorLocation {
                node: Some(node.clone()),
                actors: node_actors.remove(&node.id).unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        Ok(Json(actors))
    }

    pub async fn list_fragments(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<PbTableFragments>>> {
        let table_fragments = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr
                .fragment_manager
                .get_fragment_read_guard()
                .await
                .table_fragments()
                .values()
                .map(|tf| tf.to_protobuf())
                .collect_vec(),
            MetadataManager::V2(mgr) => mgr
                .catalog_controller
                .table_fragments()
                .await
                .map_err(err)?
                .values()
                .cloned()
                .collect_vec(),
        };

        Ok(Json(table_fragments))
    }

    async fn dump_await_tree_inner(
        worker_nodes: impl IntoIterator<Item = &WorkerNode>,
        compute_clients: &ComputeClientPool,
    ) -> Result<Json<StackTraceResponse>> {
        let mut all = Default::default();

        fn merge(a: &mut StackTraceResponse, b: StackTraceResponse) {
            a.actor_traces.extend(b.actor_traces);
            a.rpc_traces.extend(b.rpc_traces);
            a.compaction_task_traces.extend(b.compaction_task_traces);
        }

        for worker_node in worker_nodes {
            let client = compute_clients.get(worker_node).await.map_err(err)?;
            let result = client.stack_trace().await.map_err(err)?;

            merge(&mut all, result);
        }

        Ok(all.into())
    }

    pub async fn dump_await_tree_all(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<StackTraceResponse>> {
        let worker_nodes = srv
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), None)
            .await
            .map_err(err)?;

        dump_await_tree_inner(&worker_nodes, &srv.compute_clients).await
    }

    pub async fn dump_await_tree(
        Path(worker_id): Path<WorkerId>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<StackTraceResponse>> {
        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        dump_await_tree_inner(std::iter::once(&worker_node), &srv.compute_clients).await
    }

    pub async fn heap_profile(
        Path(worker_id): Path<WorkerId>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<HeapProfilingResponse>> {
        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        let client = srv.compute_clients.get(&worker_node).await.map_err(err)?;

        let result = client.heap_profile("".to_string()).await.map_err(err)?;

        Ok(result.into())
    }

    pub async fn list_heap_profile(
        Path(worker_id): Path<WorkerId>,
        Extension(srv): Extension<Service>,
    ) -> Result<Json<ListHeapProfilingResponse>> {
        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        let client = srv.compute_clients.get(&worker_node).await.map_err(err)?;

        let result = client.list_heap_profile().await.map_err(err)?;
        Ok(result.into())
    }

    pub async fn analyze_heap(
        Path((worker_id, file_path)): Path<(WorkerId, String)>,
        Extension(srv): Extension<Service>,
    ) -> Result<Response> {
        if srv.ui_path.is_none() {
            bail!("Should provide ui_path");
        }

        let file_path =
            String::from_utf8(base64_url::decode(&file_path).map_err(err)?).map_err(err)?;

        let file_name = FilePath::new(&file_path)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let collapsed_file_name = format!("{}.{}", file_name, COLLAPSED_SUFFIX);

        let worker_node = srv
            .metadata_manager
            .get_worker_by_id(worker_id)
            .await
            .map_err(err)?
            .context("worker node not found")
            .map_err(err)?;

        let client = srv.compute_clients.get(&worker_node).await.map_err(err)?;

        let collapsed_bin = client
            .analyze_heap(file_path.clone())
            .await
            .map_err(err)?
            .result;
        let collapsed_str = String::from_utf8_lossy(&collapsed_bin).to_string();

        let response = Response::builder()
            .header("Content-Type", "application/octet-stream")
            .header("Content-Disposition", collapsed_file_name)
            .body(boxed(collapsed_str));

        response.map_err(err)
    }

    pub async fn diagnose(Extension(srv): Extension<Service>) -> Result<String> {
        let report = if let Some(cmd) = &srv.diagnose_command {
            cmd.report().await
        } else {
            "Not supported in sql-backend".to_string()
        };

        Ok(report)
    }
}

impl DashboardService {
    pub async fn serve(self) -> Result<()> {
        use handlers::*;
        let ui_path = self.ui_path.clone();
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::GET]);

        let api_router = Router::new()
            .route("/clusters/:ty", get(list_clusters))
            .route("/actors", get(list_actors))
            .route("/fragments2", get(list_fragments))
            .route("/views", get(list_views))
            .route("/materialized_views", get(list_materialized_views))
            .route("/tables", get(list_tables))
            .route("/indexes", get(list_indexes))
            .route("/internal_tables", get(list_internal_tables))
            .route("/sources", get(list_sources))
            .route("/sinks", get(list_sinks))
            .route("/metrics/cluster", get(prometheus::list_prometheus_cluster))
            .route(
                "/metrics/actor/back_pressures",
                get(prometheus::list_prometheus_actor_back_pressure),
            )
            .route("/monitor/await_tree/:worker_id", get(dump_await_tree))
            .route("/monitor/await_tree/", get(dump_await_tree_all))
            .route("/monitor/dump_heap_profile/:worker_id", get(heap_profile))
            .route(
                "/monitor/list_heap_profile/:worker_id",
                get(list_heap_profile),
            )
            .route("/monitor/analyze/:worker_id/*path", get(analyze_heap))
            .route("/monitor/diagnose/", get(diagnose))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        let app = if let Some(ui_path) = ui_path {
            let static_file_router = Router::new().nest_service(
                "/",
                get_service(ServeDir::new(ui_path)).handle_error(|e| async move {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Unhandled internal error: {e}",),
                    )
                }),
            );
            Router::new()
                .fallback_service(static_file_router)
                .nest("/api", api_router)
        } else {
            let cache = Arc::new(Mutex::new(HashMap::new()));
            let service = tower::service_fn(move |req: Request<Body>| {
                let cache = cache.clone();
                async move {
                    proxy::proxy(req, cache).await.or_else(|err| {
                        Ok((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", err),
                        )
                            .into_response())
                    })
                }
            });
            Router::new()
                .fallback_service(service)
                .nest("/api", api_router)
        };

        axum::Server::bind(&srv.dashboard_addr)
            .serve(app.into_make_service())
            .await
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }
}
