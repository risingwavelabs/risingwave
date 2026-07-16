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

use std::collections::HashMap;

use risingwave_meta_model::{FragmentId, WorkerId};
use risingwave_pb::meta::{PbTableCacheRefillPolicies, PbTableRefillRuntimeConfig};

use crate::controller::fragment::FragmentParallelismInfo;
use crate::manager::NotificationVersion;
use crate::serving::{ServingVnodeMappingRef, to_pb_serving_table_vnode_mappings};

pub fn build_hummock_table_refill_runtime_config(
    serving_vnode_mapping: &ServingVnodeMappingRef,
    worker_id: WorkerId,
    table_cache_refill_policies: PbTableCacheRefillPolicies,
    streaming_parallelisms: &HashMap<FragmentId, FragmentParallelismInfo>,
    version: NotificationVersion,
) -> PbTableRefillRuntimeConfig {
    let table_vnode_mapping = serving_vnode_mapping
        .table_vnode_mappings_by_worker([worker_id], streaming_parallelisms)
        .remove(&worker_id)
        .expect("requested worker must have a table vnode mapping");

    PbTableRefillRuntimeConfig {
        table_cache_refill_policies: Some(table_cache_refill_policies),
        serving_table_vnode_mappings: Some(to_pb_serving_table_vnode_mappings(
            &table_vnode_mapping,
        )),
        version,
    }
}
