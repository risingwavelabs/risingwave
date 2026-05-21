// Copyright 2026 RisingWave Labs
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

use risingwave_pb::meta::{PbServingTableVnodeMappings, PbTableRefillRuntimeConfig};

use crate::MetaResult;
use crate::manager::{MetadataManager, NotificationVersion, WorkerKey};
use crate::serving::{
    ServingVnodeMappingRef, build_table_vnode_mapping_for_worker,
    to_pb_serving_table_vnode_mappings,
};

async fn build_hummock_serving_table_vnode_mappings(
    metadata_manager: &MetadataManager,
    serving_vnode_mapping: &ServingVnodeMappingRef,
    worker_key: &WorkerKey,
) -> MetaResult<PbServingTableVnodeMappings> {
    let active_serving_workers = metadata_manager
        .cluster_controller
        .list_active_serving_workers()
        .await?;
    let Some(worker_id) = active_serving_workers
        .iter()
        .find(|worker| worker.host.as_ref() == Some(&worker_key.0))
        .map(|worker| worker.id)
    else {
        return Ok(PbServingTableVnodeMappings::default());
    };

    let streaming_parallelisms = metadata_manager
        .catalog_controller
        .running_fragment_parallelisms(None)?;
    let serving_vnode_mappings = serving_vnode_mapping.all();
    let table_vnode_mapping = build_table_vnode_mapping_for_worker(
        worker_id,
        &serving_vnode_mappings,
        &streaming_parallelisms,
    );

    Ok(to_pb_serving_table_vnode_mappings(&table_vnode_mapping))
}

pub async fn build_hummock_table_refill_runtime_config(
    metadata_manager: &MetadataManager,
    serving_vnode_mapping: &ServingVnodeMappingRef,
    worker_key: &WorkerKey,
    version: NotificationVersion,
) -> MetaResult<PbTableRefillRuntimeConfig> {
    let table_cache_refill_policies = metadata_manager
        .catalog_controller
        .table_cache_refill_policies_snapshot()
        .await?;
    let serving_table_vnode_mappings = build_hummock_serving_table_vnode_mappings(
        metadata_manager,
        serving_vnode_mapping,
        worker_key,
    )
    .await?;

    Ok(PbTableRefillRuntimeConfig {
        table_cache_refill_policies: Some(table_cache_refill_policies),
        serving_table_vnode_mappings: Some(serving_table_vnode_mappings),
        version,
    })
}
