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

//! Current table cache policies and vnode mappings for one Hummock instance.
//! The event handler owns and updates this state; refill tasks consume immutable snapshots.

use std::collections::HashMap;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_pb::id::TableId;

pub(crate) struct TableCacheMetadata {
    pub role: Role,
    pub default_policy: CacheRefillPolicy,
    /// Per-table overrides; an absent entry uses `default_policy`.
    pub table_cache_refill_policies: HashMap<TableId, CacheRefillPolicy>,
    /// Union of vnodes from this worker's local read versions, maintained by the handler.
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    /// Complete worker-specific serving mapping received from Meta.
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

impl TableCacheMetadata {
    pub(crate) fn new(role: Role, default_policy: CacheRefillPolicy) -> Self {
        Self {
            role,
            default_policy,
            table_cache_refill_policies: HashMap::new(),
            streaming_table_vnode_mapping: HashMap::new(),
            serving_table_vnode_mapping: HashMap::new(),
        }
    }

    pub(crate) fn update_streaming_table_vnodes(
        &mut self,
        table_id: TableId,
        streaming_vnodes: Option<Bitmap>,
    ) {
        if let Some(streaming_vnodes) = streaming_vnodes {
            self.streaming_table_vnode_mapping
                .insert(table_id, streaming_vnodes);
        } else {
            self.streaming_table_vnode_mapping.remove(&table_id);
        }
    }
}
