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

use std::net::SocketAddr;
use std::path::Path as FilePath;
use std::sync::Arc;

use anyhow::{anyhow, Context as _, Result};
use axum::extract::{Extension, Path};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use risingwave_common::util::StackTraceResponseExt;
use risingwave_rpc_client::ComputeClientPool;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::manager::diagnose::DiagnoseCommandRef;
use crate::manager::MetadataManager;

#[derive(Clone)]
pub struct DashboardService {
    pub dashboard_addr: SocketAddr,
    pub prometheus_client: Option<prometheus_http_query::Client>,
    pub prometheus_selector: String,
    pub metadata_manager: MetadataManager,
    pub compute_clients: ComputeClientPool,
    pub diagnose_command: DiagnoseCommandRef,
    pub trace_state: otlp_embedded::StateRef,
}

pub type Service = Arc<DashboardService>;

pub(super) mod handlers {
    use std::collections::HashMap;

    use anyhow::Context;
    use axum::Json;
    use futures::future::join_all;
    use itertools::Itertools;
    use risingwave_common::bail;
    use risingwave_common::catalog::TableId;
    use risingwave_common_heap_profiling::COLLAPSED_SUFFIX;
    use risingwave_pb::catalog::table::TableType;
    use risingwave_pb::catalog::{PbDatabase, PbSchema, Sink, Source, Subscription, Table, View};
    use risingwave_pb::common::{WorkerNode, WorkerType};
    use risingwave_pb::meta::list_object_dependencies_response::PbObjectDependencies;
    use risingwave_pb::meta::{
        ActorIds, FragmentIdToActorIdMap, FragmentVertexToRelationMap, PbTableFragments,
        RelationIdInfos,
    };
    use risingwave_pb::monitor_service::{
        GetBackPressureResponse, HeapProfilingResponse, ListHeapProfilingResponse,
        StackTraceResponse,
    };
    use risingwave_pb::stream_plan::FragmentTypeFlag;
    use risingwave_pb::user::PbUserInfo;
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

    pub async fn list_subscription(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<Subscription>>> {
        let subscriptions = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_subscriptions().await,
            MetadataManager::V2(mgr) => mgr
                .catalog_controller
                .list_subscriptions()
                .await
                .map_err(err)?,
        };

        Ok(Json(subscriptions))
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

    /// In the ddl backpressure graph, we want to compute the backpressure between relations.
    /// So we need to know which are the fragments which are connected to external relations.
    /// These fragments form the vertices of the graph.
    /// We can get collection of backpressure values, keyed by vertex_id-vertex_id.
    /// This function will return a map of fragment vertex id to relation id.
    /// We can convert `vertex_id-vertex_id` to `relation_id-relation_id` using that.
    /// Finally, we have a map of `relation_id-relation_id` to backpressure values.
    pub async fn get_fragment_vertex_to_relation_id_map(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<FragmentVertexToRelationMap>> {
        let map = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => {
                let core = mgr.fragment_manager.get_fragment_read_guard().await;
                let table_fragments = core.table_fragments();
                let mut in_map = HashMap::new();
                let mut out_map = HashMap::new();
                for (relation_id, tf) in table_fragments {
                    for (fragment_id, fragment) in &tf.fragments {
                        if (fragment.fragment_type_mask & FragmentTypeFlag::StreamScan as u32) != 0
                            || (fragment.fragment_type_mask
                                & FragmentTypeFlag::SnapshotBackfillStreamScan as u32)
                                != 0
                        {
                            in_map.insert(*fragment_id, relation_id.table_id);
                        }
                        if (fragment.fragment_type_mask & FragmentTypeFlag::Mview as u32) != 0 {
                            out_map.insert(*fragment_id, relation_id.table_id);
                        }
                    }
                }
                FragmentVertexToRelationMap { in_map, out_map }
            }
            MetadataManager::V2(mgr) => {
                let table_fragments = mgr
                    .catalog_controller
                    .table_fragments()
                    .await
                    .map_err(err)?;
                let mut in_map = HashMap::new();
                let mut out_map = HashMap::new();
                for (relation_id, tf) in table_fragments {
                    for (fragment_id, fragment) in &tf.fragments {
                        if (fragment.fragment_type_mask & FragmentTypeFlag::StreamScan as u32) != 0
                            || (fragment.fragment_type_mask
                                & FragmentTypeFlag::SnapshotBackfillStreamScan as u32)
                                != 0
                        {
                            in_map.insert(*fragment_id, relation_id as u32);
                        }
                        if (fragment.fragment_type_mask & FragmentTypeFlag::Mview as u32) != 0 {
                            out_map.insert(*fragment_id, relation_id as u32);
                        }
                    }
                }
                FragmentVertexToRelationMap { in_map, out_map }
            }
        };
        Ok(Json(map))
    }

    /// Provides a hierarchy of relation ids to fragments to actors.
    pub async fn get_relation_id_infos(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<RelationIdInfos>> {
        let map = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => {
                let core = mgr.fragment_manager.get_fragment_read_guard().await;
                let table_fragments = core.table_fragments();
                let mut map = HashMap::new();
                for (id, tf) in table_fragments {
                    let mut fragment_id_to_actor_ids = HashMap::new();
                    for (fragment_id, fragment) in &tf.fragments {
                        let actor_ids = fragment.actors.iter().map(|a| a.actor_id).collect_vec();
                        fragment_id_to_actor_ids.insert(*fragment_id, ActorIds { ids: actor_ids });
                    }
                    map.insert(
                        id.table_id,
                        FragmentIdToActorIdMap {
                            map: fragment_id_to_actor_ids,
                        },
                    );
                }
                map
            }
            MetadataManager::V2(mgr) => {
                let table_fragments = mgr
                    .catalog_controller
                    .table_fragments()
                    .await
                    .map_err(err)?;
                let mut map = HashMap::new();
                for (id, tf) in table_fragments {
                    let mut fragment_id_to_actor_ids = HashMap::new();
                    for (fragment_id, fragment) in &tf.fragments {
                        let actor_ids = fragment.actors.iter().map(|a| a.actor_id).collect_vec();
                        fragment_id_to_actor_ids.insert(*fragment_id, ActorIds { ids: actor_ids });
                    }
                    map.insert(
                        id as u32,
                        FragmentIdToActorIdMap {
                            map: fragment_id_to_actor_ids,
                        },
                    );
                }
                map
            }
        };
        let relation_id_infos = RelationIdInfos { map };

        Ok(Json(relation_id_infos))
    }

    pub async fn list_fragments_by_job_id(
        Extension(srv): Extension<Service>,
        Path(job_id): Path<u32>,
    ) -> Result<Json<PbTableFragments>> {
        let table_fragments = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => {
                if let Some(tf) = mgr
                    .fragment_manager
                    .get_fragment_read_guard()
                    .await
                    .table_fragments()
                    .get(&TableId::new(job_id))
                {
                    tf.to_protobuf()
                } else {
                    bail!("job_id {} not found", job_id)
                }
            }
            MetadataManager::V2(mgr) => {
                let mut table_fragments = mgr
                    .catalog_controller
                    .table_fragments()
                    .await
                    .map_err(err)?;
                if let Some(tf) = table_fragments.remove(&(job_id as i32)) {
                    tf
                } else {
                    bail!("job_id {} not found", job_id)
                }
            }
        };

        Ok(Json(table_fragments))
    }

    pub async fn list_users(Extension(srv): Extension<Service>) -> Result<Json<Vec<PbUserInfo>>> {
        let users = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_users().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_users().await.map_err(err)?,
        };

        Ok(Json(users))
    }

    pub async fn list_databases(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<PbDatabase>>> {
        let databases = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_databases().await,
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller.list_databases().await.map_err(err)?
            }
        };

        Ok(Json(databases))
    }

    pub async fn list_schemas(Extension(srv): Extension<Service>) -> Result<Json<Vec<PbSchema>>> {
        let schemas = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_schemas().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_schemas().await.map_err(err)?,
        };

        Ok(Json(schemas))
    }

    pub async fn list_object_dependencies(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<Vec<PbObjectDependencies>>> {
        let object_dependencies = match &srv.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_object_dependencies().await,
            MetadataManager::V2(mgr) => mgr
                .catalog_controller
                .list_object_dependencies()
                .await
                .map_err(err)?,
        };

        Ok(Json(object_dependencies))
    }

    async fn dump_await_tree_inner(
        worker_nodes: impl IntoIterator<Item = &WorkerNode>,
        compute_clients: &ComputeClientPool,
    ) -> Result<Json<StackTraceResponse>> {
        let mut all = StackTraceResponse::default();

        for worker_node in worker_nodes {
            let client = compute_clients.get(worker_node).await.map_err(err)?;
            let result = client.stack_trace().await.map_err(err)?;

            all.merge_other(result);
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
            .body(collapsed_str.into());

        response.map_err(err)
    }

    pub async fn diagnose(Extension(srv): Extension<Service>) -> Result<String> {
        Ok(srv.diagnose_command.report().await)
    }

    /// NOTE(kwannoel): Although we fetch the BP for the entire graph via this API,
    /// the workload should be reasonable.
    /// In most cases, we can safely assume each node has most 2 outgoing edges (e.g. join).
    /// In such a scenario, the number of edges is linear to the number of nodes.
    /// So the workload is proportional to the relation id graph we fetch in `get_relation_id_infos`.
    pub async fn get_embedded_back_pressures(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<GetBackPressureResponse>> {
        let worker_nodes = srv
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .map_err(err)?;

        let mut futures = Vec::new();

        for worker_node in worker_nodes {
            let client = srv.compute_clients.get(&worker_node).await.map_err(err)?;
            let client = Arc::new(client);
            let fut = async move {
                let result = client.get_back_pressure().await.map_err(err)?;
                Ok::<_, DashboardError>(result)
            };
            futures.push(fut);
        }
        let results = join_all(futures).await;

        let mut all = GetBackPressureResponse::default();

        for result in results {
            let result = result
                .map_err(|_| anyhow!("Failed to get back pressure"))
                .map_err(err)?;
            all.back_pressure_infos.extend(result.back_pressure_infos);
        }

        Ok(all.into())
    }

    pub async fn get_version(Extension(_srv): Extension<Service>) -> Result<Json<String>> {
        Ok(Json(risingwave_common::current_cluster_version()))
    }
}

impl DashboardService {
    pub async fn serve(self) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::GET]);

        let api_router = Router::new()
            .route("/version", get(get_version))
            .route("/clusters/:ty", get(list_clusters))
            .route("/fragments2", get(list_fragments))
            .route("/fragments/job_id/:job_id", get(list_fragments_by_job_id))
            .route("/relation_id_infos", get(get_relation_id_infos))
            .route(
                "/fragment_vertex_to_relation_id_map",
                get(get_fragment_vertex_to_relation_id_map),
            )
            .route("/views", get(list_views))
            .route("/materialized_views", get(list_materialized_views))
            .route("/tables", get(list_tables))
            .route("/indexes", get(list_indexes))
            .route("/subscriptions", get(list_subscription))
            .route("/internal_tables", get(list_internal_tables))
            .route("/sources", get(list_sources))
            .route("/sinks", get(list_sinks))
            .route("/users", get(list_users))
            .route("/databases", get(list_databases))
            .route("/schemas", get(list_schemas))
            .route("/object_dependencies", get(list_object_dependencies))
            .route("/metrics/cluster", get(prometheus::list_prometheus_cluster))
            .route(
                "/metrics/fragment/prometheus_back_pressures",
                get(prometheus::list_prometheus_fragment_back_pressure),
            )
            .route(
                "/metrics/fragment/embedded_back_pressures",
                get(get_embedded_back_pressures),
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

        let trace_ui_router = otlp_embedded::ui_app(srv.trace_state.clone(), "/trace/");
        let dashboard_router = risingwave_meta_dashboard::router();

        let app = Router::new()
            .fallback_service(dashboard_router)
            .nest("/api", api_router)
            .nest("/trace", trace_ui_router)
            .layer(CompressionLayer::new());

        let listener = TcpListener::bind(&srv.dashboard_addr)
            .await
            .context("failed to bind dashboard address")?;
        axum::serve(listener, app)
            .await
            .context("failed to serve dashboard service")?;

        Ok(())
    }
}
