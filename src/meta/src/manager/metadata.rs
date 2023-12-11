// Copyright 2023 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::{TableId, TableOption};
use risingwave_pb::common::worker_node::{PbResource, State};
use risingwave_pb::common::{HostAddress, PbWorkerNode, PbWorkerType, WorkerType};
use risingwave_pb::meta::add_worker_node_request::Property as AddNodeProperty;

use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::{ClusterControllerRef, WorkerExtraInfo};
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, StreamingClusterInfo, WorkerId,
};
use crate::model::{MetadataModel, TableFragments};
use crate::MetaResult;

#[derive(Clone)]
pub enum MetadataFucker {
    V1(MetadataFuckerV1),
    V2(MetadataFuckerV2),
}

#[derive(Clone)]
pub struct MetadataFuckerV1 {
    pub cluster_manager: ClusterManagerRef,
    pub catalog_manager: CatalogManagerRef,
    pub fragment_manager: FragmentManagerRef,
}

#[derive(Clone)]
pub struct MetadataFuckerV2 {
    pub cluster_controller: ClusterControllerRef,
    pub catalog_controller: CatalogControllerRef,
}

impl MetadataFucker {
    pub fn new_v1(
        cluster_manager: ClusterManagerRef,
        catalog_manager: CatalogManagerRef,
        fragment_manager: FragmentManagerRef,
    ) -> Self {
        Self::V1(MetadataFuckerV1 {
            cluster_manager,
            catalog_manager,
            fragment_manager,
        })
    }

    pub fn new_v2(
        cluster_controller: ClusterControllerRef,
        catalog_controller: CatalogControllerRef,
    ) -> Self {
        Self::V2(MetadataFuckerV2 {
            cluster_controller,
            catalog_controller,
        })
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        match &self {
            MetadataFucker::V1(fucker) => Ok(fucker
                .cluster_manager
                .get_worker_by_id(worker_id)
                .await
                .map(|w| w.worker_node)),
            MetadataFucker::V2(fucker) => {
                fucker
                    .cluster_controller
                    .get_worker_by_id(worker_id as _)
                    .await
            }
        }
    }

    pub async fn get_worker_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        match &self {
            MetadataFucker::V1(fucker) => fucker
                .cluster_manager
                .get_worker_by_id(worker_id)
                .await
                .map(Into::into),
            MetadataFucker::V2(fucker) => {
                fucker
                    .cluster_controller
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
            MetadataFucker::V1(fucker) => fucker
                .cluster_manager
                .add_worker_node(r#type, host_address, property, resource)
                .await
                .map(|w| w.id),
            MetadataFucker::V2(fucker) => fucker
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
            MetadataFucker::V1(fucker) => Ok(fucker
                .cluster_manager
                .list_worker_node(worker_type, worker_state)
                .await),
            MetadataFucker::V2(fucker) => {
                fucker
                    .cluster_controller
                    .list_workers(worker_type.map(Into::into), worker_state.map(Into::into))
                    .await
            }
        }
    }

    pub async fn list_active_streaming_compute_nodes(&self) -> MetaResult<Vec<PbWorkerNode>> {
        match self {
            MetadataFucker::V1(fucker) => Ok(fucker
                .cluster_manager
                .list_active_streaming_compute_nodes()
                .await),
            MetadataFucker::V2(fucker) => {
                fucker
                    .cluster_controller
                    .list_active_streaming_workers()
                    .await
            }
        }
    }

    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        match self {
            MetadataFucker::V1(fucker) => {
                Ok(fucker.cluster_manager.get_streaming_cluster_info().await)
            }
            MetadataFucker::V2(fucker) => {
                fucker.cluster_controller.get_streaming_cluster_info().await
            }
        }
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<u32, TableOption>> {
        match &self {
            MetadataFucker::V1(fucker) => Ok(fucker.catalog_manager.get_all_table_options().await),
            MetadataFucker::V2(fucker) => fucker
                .catalog_controller
                .get_all_table_options()
                .await
                .map(|tops| tops.into_iter().map(|(id, opt)| (id as u32, opt)).collect()),
        }
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<u32>> {
        match &self {
            MetadataFucker::V1(fucker) => Ok(fucker.catalog_manager.get_created_table_ids().await),
            MetadataFucker::V2(fucker) => {
                let table_ids = fucker.catalog_controller.get_created_table_ids().await?;
                Ok(table_ids.into_iter().map(|id| id as u32).collect())
            }
        }
    }

    pub async fn get_job_id_to_internal_table_ids_mapping(&self) -> Option<Vec<(u32, Vec<u32>)>> {
        match &self {
            MetadataFucker::V1(fucker) => fucker
                .fragment_manager
                .get_mv_id_to_internal_table_ids_mapping(),
            MetadataFucker::V2(fucker) => {
                let job_internal_table_ids =
                    fucker.catalog_controller.get_job_internal_table_ids().await;
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
            MetadataFucker::V1(fucker) => {
                fucker
                    .fragment_manager
                    .select_table_fragments_by_table_id(id)
                    .await
            }
            MetadataFucker::V2(fucker) => {
                let pb_table_fragments = fucker
                    .catalog_controller
                    .get_job_fragments_by_id(id.table_id as _)
                    .await?;
                Ok(TableFragments::from_protobuf(pb_table_fragments))
            }
        }
    }

    pub async fn get_job_fragments_by_ids(
        &self,
        ids: &[TableId],
    ) -> MetaResult<Vec<TableFragments>> {
        match self {
            MetadataFucker::V1(fucker) => {
                fucker
                    .fragment_manager
                    .select_table_fragments_by_ids(ids)
                    .await
            }
            MetadataFucker::V2(fucker) => {
                let mut table_fragments = vec![];
                for id in ids {
                    let pb_table_fragments = fucker
                        .catalog_controller
                        .get_job_fragments_by_id(id.table_id as _)
                        .await?;
                    table_fragments.push(TableFragments::from_protobuf(pb_table_fragments));
                }
                Ok(table_fragments)
            }
        }
    }

    pub async fn drop_streaming_job_by_ids(&self, table_ids: &HashSet<TableId>) -> MetaResult<()> {
        match self {
            MetadataFucker::V1(fucker) => {
                fucker
                    .fragment_manager
                    .drop_table_fragments_vec(table_ids)
                    .await
            }
            MetadataFucker::V2(_) => {
                // Do nothing. Need to refine drop and cancel process.
                Ok(())
            }
        }
    }
}
