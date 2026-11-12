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
//! The event handler is the sole writer. Foyer captures immutable task snapshots;
//! Pin reads current ownership under a guard when admitting asynchronous work.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_pb::id::TableId;
use risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy;
use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;

/// Lock order: metadata, Pin pending queue, executor/cache locks. Never hold across I/O or await.
pub(crate) type SharedTableCacheMetadata = Arc<RwLock<TableCacheMetadata>>;

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

    /// Borrows this table's owned vnode bitmaps for the worker's active roles.
    pub(crate) fn owned_vnodes(&self, table: &TableId) -> impl Iterator<Item = &Bitmap> {
        [
            self.streaming_table_vnode_mapping
                .get(table)
                .filter(|_| self.role.for_streaming()),
            self.serving_table_vnode_mapping
                .get(table)
                .filter(|_| self.role.for_serving()),
        ]
        .into_iter()
        .flatten()
    }

    pub(crate) fn has_vnodes(&self, table: &TableId) -> bool {
        self.owned_vnodes(table).any(Bitmap::any)
    }

    pub(crate) fn contains_vnode(&self, table: &TableId, vnode: usize) -> bool {
        self.owned_vnodes(table)
            .any(|bitmap| vnode < bitmap.len() && bitmap.is_set(vnode))
    }

    pub(crate) fn is_pinned(&self, table: &TableId) -> bool {
        self.policy(table) == CacheRefillPolicy::Pin
    }

    pub(crate) fn policy(&self, table: &TableId) -> CacheRefillPolicy {
        self.table_cache_refill_policies
            .get(table)
            .copied()
            .unwrap_or(self.default_policy)
    }

    /// Replaces policy metadata and returns the previous overrides.
    /// Consumers decide whether the change invalidates their work.
    pub(crate) fn replace_policy_snapshot(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
    ) -> HashMap<TableId, CacheRefillPolicy> {
        std::mem::replace(&mut self.table_cache_refill_policies, policies)
    }

    /// Returns the replaced mapping without interpreting the ownership change.
    pub(crate) fn replace_serving_mapping(
        &mut self,
        mapping: HashMap<TableId, Bitmap>,
    ) -> HashMap<TableId, Bitmap> {
        std::mem::replace(&mut self.serving_table_vnode_mapping, mapping)
    }

    /// Returns the table's previous vnode bitmap, if present.
    pub(crate) fn update_streaming_table_vnodes(
        &mut self,
        table: TableId,
        bitmap: Option<Bitmap>,
    ) -> Option<Bitmap> {
        if let Some(bitmap) = bitmap {
            self.streaming_table_vnode_mapping.insert(table, bitmap)
        } else {
            self.streaming_table_vnode_mapping.remove(&table)
        }
    }
}

pub(crate) fn table_cache_refill_policy_from_protobuf(
    pb_policy: &PbTableCacheRefillPolicy,
) -> Option<(TableId, CacheRefillPolicy)> {
    let table_id = TableId::from(pb_policy.table_id);
    let policy = match PbCacheRefillPolicy::try_from(pb_policy.policy) {
        Ok(policy) => CacheRefillPolicy::from_protobuf(policy)?,
        Err(_) => {
            tracing::warn!(
                table_id = pb_policy.table_id,
                policy = pb_policy.policy,
                "ignore unknown table cache refill policy"
            );
            return None;
        }
    };
    Some((table_id, policy))
}
