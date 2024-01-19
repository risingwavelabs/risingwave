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

use std::collections::{BTreeMap, HashMap, HashSet};

use risingwave_common::catalog::{TableId, TableOption};
use risingwave_meta_model_v2::SourceId;
use risingwave_pb::catalog::{PbSource, PbTable};
use risingwave_pb::common::worker_node::{PbResource, State};
use risingwave_pb::common::{HostAddress, PbWorkerNode, PbWorkerType, WorkerType};
use risingwave_pb::meta::add_worker_node_request::Property as AddNodeProperty;
use risingwave_pb::meta::table_fragments::{Fragment, PbFragment};
use risingwave_pb::stream_plan::{PbDispatchStrategy, PbStreamActor};

use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::{ClusterControllerRef, WorkerExtraInfo};
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, StreamingClusterInfo, WorkerId,
};
use crate::model::{ActorId, FragmentId, MetadataModel, TableFragments};
use crate::stream::SplitAssignment;
use crate::MetaResult;

#[derive(Clone)]
pub enum MetadataManager {
    V1(MetadataManagerV1),
    V2(MetadataManagerV2),
}

#[derive(Clone)]
pub struct MetadataManagerV1 {
    pub cluster_manager: ClusterManagerRef,
    pub catalog_manager: CatalogManagerRef,
    pub fragment_manager: FragmentManagerRef,
}

#[derive(Clone)]
pub struct MetadataManagerV2 {
    pub cluster_controller: ClusterControllerRef,
    pub catalog_controller: CatalogControllerRef,
}

impl MetadataManager {
    pub fn new_v1(
        cluster_manager: ClusterManagerRef,
        catalog_manager: CatalogManagerRef,
        fragment_manager: FragmentManagerRef,
    ) -> Self {
        Self::V1(MetadataManagerV1 {
            cluster_manager,
            catalog_manager,
            fragment_manager,
        })
    }

    pub fn new_v2(
        cluster_controller: ClusterControllerRef,
        catalog_controller: CatalogControllerRef,
    ) -> Self {
        Self::V2(MetadataManagerV2 {
            cluster_controller,
            catalog_controller,
        })
    }

    pub fn as_v1_ref(&self) -> &MetadataManagerV1 {
        match self {
            MetadataManager::V1(mgr) => mgr,
            MetadataManager::V2(_) => panic!("expect v1, found v2"),
        }
    }

    pub fn as_v2_ref(&self) -> &MetadataManagerV2 {
        match self {
            MetadataManager::V1(_) => panic!("expect v2, found v1"),
            MetadataManager::V2(mgr) => mgr,
        }
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        match &self {
            MetadataManager::V1(mgr) => Ok(mgr
                .cluster_manager
                .get_worker_by_id(worker_id)
                .await
                .map(|w| w.worker_node)),
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller
                    .get_worker_by_id(worker_id as _)
                    .await
            }
        }
    }

    pub async fn count_worker_node(&self) -> MetaResult<HashMap<WorkerType, u64>> {
        match &self {
            MetadataManager::V1(mgr) => Ok(mgr.cluster_manager.count_worker_node().await),
            MetadataManager::V2(mgr) => {
                let node_map = mgr.cluster_controller.count_worker_by_type().await?;
                Ok(node_map
                    .into_iter()
                    .map(|(ty, cnt)| (ty.into(), cnt as u64))
                    .collect())
            }
        }
    }

    pub async fn get_worker_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        match &self {
            MetadataManager::V1(mgr) => mgr
                .cluster_manager
                .get_worker_by_id(worker_id)
                .await
                .map(Into::into),
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller
                    .get_worker_info_by_id(worker_id as _)
                    .await
            }
        }
    }

    pub async fn add_worker_node(
        &self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        property: AddNodeProperty,
        resource: PbResource,
    ) -> MetaResult<WorkerId> {
        match &self {
            MetadataManager::V1(mgr) => mgr
                .cluster_manager
                .add_worker_node(r#type, host_address, property, resource)
                .await
                .map(|w| w.id),
            MetadataManager::V2(mgr) => mgr
                .cluster_controller
                .add_worker(r#type, host_address, property, resource)
                .await
                .map(|id| id as WorkerId),
        }
    }

    pub async fn list_worker_node(
        &self,
        worker_type: Option<WorkerType>,
        worker_state: Option<State>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        match &self {
            MetadataManager::V1(mgr) => Ok(mgr
                .cluster_manager
                .list_worker_node(worker_type, worker_state)
                .await),
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller
                    .list_workers(worker_type.map(Into::into), worker_state.map(Into::into))
                    .await
            }
        }
    }

    pub async fn list_active_streaming_compute_nodes(&self) -> MetaResult<Vec<PbWorkerNode>> {
        match self {
            MetadataManager::V1(mgr) => Ok(mgr
                .cluster_manager
                .list_active_streaming_compute_nodes()
                .await),
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller.list_active_streaming_workers().await
            }
        }
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        match self {
            MetadataManager::V1(mgr) => Ok(mgr.catalog_manager.list_sources().await),
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_sources().await,
        }
    }

    /// Get and filter the "**root**" fragments of the specified relations.
    /// The root fragment is the bottom-most fragment of its fragment graph, and can be a `MView` or a `Source`.
    ///
    /// ## What can be the root fragment
    /// - For MV, it should have one `MView` fragment.
    /// - For table, it should have one `MView` fragment and one or two `Source` fragments. `MView` should be the root.
    /// - For source, it should have one `Source` fragment.
    ///
    /// In other words, it's the `MView` fragment if it exists, otherwise it's the `Source` fragment.
    ///
    /// ## What do we expect to get for different creating streaming job
    /// - MV/Sink/Index should have MV upstream fragments for upstream MV/Tables, and Source upstream fragments for upstream backfill-able sources.
    /// - CDC Table has a Source upstream fragment.
    /// - Sources and other Tables shouldn't have an upstream fragment.
    pub async fn get_upstream_root_fragments(
        &self,
        upstream_table_ids: &HashSet<TableId>,
    ) -> MetaResult<HashMap<TableId, Fragment>> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .get_upstream_root_fragments(upstream_table_ids)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let upstream_root_fragments = mgr
                    .catalog_controller
                    .get_upstream_root_fragments(
                        upstream_table_ids
                            .iter()
                            .map(|id| id.table_id as _)
                            .collect(),
                    )
                    .await?;
                Ok(upstream_root_fragments
                    .into_iter()
                    .map(|(id, fragment)| ((id as u32).into(), fragment))
                    .collect())
            }
        }
    }

    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        match self {
            MetadataManager::V1(mgr) => Ok(mgr.cluster_manager.get_streaming_cluster_info().await),
            MetadataManager::V2(mgr) => mgr.cluster_controller.get_streaming_cluster_info().await,
        }
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<u32, TableOption>> {
        match &self {
            MetadataManager::V1(mgr) => Ok(mgr.catalog_manager.get_all_table_options().await),
            MetadataManager::V2(mgr) => mgr
                .catalog_controller
                .get_all_table_options()
                .await
                .map(|tops| tops.into_iter().map(|(id, opt)| (id as u32, opt)).collect()),
        }
    }

    pub async fn get_table_name_type_mapping(&self) -> MetaResult<HashMap<u32, (String, String)>> {
        match &self {
            MetadataManager::V1(mgr) => {
                Ok(mgr.catalog_manager.get_table_name_and_type_mapping().await)
            }
            MetadataManager::V2(mgr) => {
                let mappings = mgr.catalog_controller.get_table_name_type_mapping().await?;
                Ok(mappings
                    .into_iter()
                    .map(|(id, value)| (id as u32, value))
                    .collect())
            }
        }
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<u32>> {
        match &self {
            MetadataManager::V1(mgr) => Ok(mgr.catalog_manager.get_created_table_ids().await),
            MetadataManager::V2(mgr) => {
                let table_ids = mgr.catalog_controller.get_created_table_ids().await?;
                Ok(table_ids.into_iter().map(|id| id as u32).collect())
            }
        }
    }

    pub async fn get_table_catalog_by_ids(&self, ids: Vec<u32>) -> MetaResult<Vec<PbTable>> {
        match &self {
            MetadataManager::V1(mgr) => Ok(mgr.catalog_manager.get_tables(&ids).await),
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .get_table_by_ids(ids.into_iter().map(|id| id as _).collect())
                    .await
            }
        }
    }

    pub async fn get_downstream_chain_fragments(
        &self,
        job_id: u32,
    ) -> MetaResult<Vec<(PbDispatchStrategy, PbFragment)>> {
        match &self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .get_downstream_fragments(job_id.into())
                    .await
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .get_downstream_chain_fragments(job_id as _)
                    .await
            }
        }
    }

    pub async fn get_worker_actor_ids(
        &self,
        job_ids: HashSet<TableId>,
    ) -> MetaResult<BTreeMap<WorkerId, Vec<ActorId>>> {
        match &self {
            MetadataManager::V1(mgr) => mgr.fragment_manager.table_node_actors(&job_ids).await,
            MetadataManager::V2(mgr) => {
                let worker_actors = mgr
                    .catalog_controller
                    .get_worker_actor_ids(job_ids.into_iter().map(|id| id.table_id as _).collect())
                    .await?;
                Ok(worker_actors
                    .into_iter()
                    .map(|(id, actors)| {
                        (
                            id as WorkerId,
                            actors.into_iter().map(|id| id as ActorId).collect(),
                        )
                    })
                    .collect())
            }
        }
    }

    pub async fn get_job_id_to_internal_table_ids_mapping(&self) -> Option<Vec<(u32, Vec<u32>)>> {
        match &self {
            MetadataManager::V1(mgr) => mgr
                .fragment_manager
                .get_mv_id_to_internal_table_ids_mapping(),
            MetadataManager::V2(mgr) => {
                let job_internal_table_ids =
                    mgr.catalog_controller.get_job_internal_table_ids().await;
                job_internal_table_ids.map(|ids| {
                    ids.into_iter()
                        .map(|(id, internal_ids)| {
                            (
                                id as u32,
                                internal_ids.into_iter().map(|id| id as u32).collect(),
                            )
                        })
                        .collect()
                })
            }
        }
    }

    pub async fn get_job_fragments_by_id(&self, id: &TableId) -> MetaResult<TableFragments> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .select_table_fragments_by_table_id(id)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let pb_table_fragments = mgr
                    .catalog_controller
                    .get_job_fragments_by_id(id.table_id as _)
                    .await?;
                Ok(TableFragments::from_protobuf(pb_table_fragments))
            }
        }
    }

    pub async fn get_running_actors_of_fragment(
        &self,
        id: FragmentId,
    ) -> MetaResult<HashSet<ActorId>> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .get_running_actors_of_fragment(id)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let actor_ids = mgr
                    .catalog_controller
                    .get_running_actors_of_fragment(id as _)
                    .await?;
                Ok(actor_ids.into_iter().map(|id| id as ActorId).collect())
            }
        }
    }

    pub async fn get_job_fragments_by_ids(
        &self,
        ids: &[TableId],
    ) -> MetaResult<Vec<TableFragments>> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .select_table_fragments_by_ids(ids)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let mut table_fragments = vec![];
                for id in ids {
                    let pb_table_fragments = mgr
                        .catalog_controller
                        .get_job_fragments_by_id(id.table_id as _)
                        .await?;
                    table_fragments.push(TableFragments::from_protobuf(pb_table_fragments));
                }
                Ok(table_fragments)
            }
        }
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> MetaResult<HashMap<WorkerId, Vec<PbStreamActor>>> {
        match &self {
            MetadataManager::V1(mgr) => {
                Ok(mgr.fragment_manager.all_node_actors(include_inactive).await)
            }
            MetadataManager::V2(mgr) => {
                let table_fragments = mgr.catalog_controller.table_fragments().await?;
                let mut actor_maps = HashMap::new();
                for (_, fragments) in table_fragments {
                    let tf = TableFragments::from_protobuf(fragments);
                    for (node_id, actor_ids) in tf.worker_actors(include_inactive) {
                        let node_actor_ids = actor_maps.entry(node_id).or_insert_with(Vec::new);
                        node_actor_ids.extend(actor_ids);
                    }
                }
                Ok(actor_maps)
            }
        }
    }

    pub async fn drop_streaming_job_by_ids(&self, table_ids: &HashSet<TableId>) -> MetaResult<()> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .drop_table_fragments_vec(table_ids)
                    .await
            }
            MetadataManager::V2(_) => {
                // Do nothing. Need to refine drop and cancel process.
                Ok(())
            }
        }
    }

    pub async fn update_source_rate_limit_by_source_id(
        &self,
        source_id: SourceId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .update_source_rate_limit_by_source_id(source_id, rate_limit)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let fragment_actors = mgr
                    .catalog_controller
                    .update_source_rate_limit_by_source_id(source_id as _, rate_limit)
                    .await?;
                Ok(fragment_actors
                    .into_iter()
                    .map(|(id, actors)| (id as _, actors.into_iter().map(|id| id as _).collect()))
                    .collect())
            }
        }
    }

    pub async fn update_mv_rate_limit_by_table_id(
        &self,
        table_id: TableId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .update_mv_rate_limit_by_table_id(table_id, rate_limit)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let fragment_actors = mgr
                    .catalog_controller
                    .update_mv_rate_limit_by_job_id(table_id.table_id as _, rate_limit)
                    .await?;
                Ok(fragment_actors
                    .into_iter()
                    .map(|(id, actors)| (id as _, actors.into_iter().map(|id| id as _).collect()))
                    .collect())
            }
        }
    }

    pub async fn update_actor_splits_by_split_assignment(
        &self,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        match self {
            MetadataManager::V1(mgr) => {
                mgr.fragment_manager
                    .update_actor_splits_by_split_assignment(split_assignment)
                    .await
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .update_actor_splits(split_assignment)
                    .await
            }
        }
    }
}
