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

use std::collections::{HashMap, HashSet};

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_pb::id::TableId;

use super::refill_locality::RefillOwnership;

pub(crate) struct TableCacheMetadata {
    pub role: Role,
    pub default_policy: CacheRefillPolicy,
    /// Per-table overrides; an absent entry uses `default_policy`.
    pub table_cache_refill_policies: HashMap<TableId, CacheRefillPolicy>,
    /// Union of vnodes from this worker's local read versions, maintained by the handler.
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    /// Complete worker-specific serving mapping received from Meta.
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
    pinned_table_ids: HashSet<TableId>,
}

impl TableCacheMetadata {
    pub(crate) fn new(role: Role, default_policy: CacheRefillPolicy) -> Self {
        Self {
            role,
            default_policy,
            table_cache_refill_policies: HashMap::new(),
            streaming_table_vnode_mapping: HashMap::new(),
            serving_table_vnode_mapping: HashMap::new(),
            pinned_table_ids: HashSet::new(),
        }
    }

    pub(crate) fn ownership(&self) -> RefillOwnership<'_> {
        RefillOwnership {
            streaming: self
                .role
                .for_streaming()
                .then_some(&self.streaming_table_vnode_mapping),
            serving: self
                .role
                .for_serving()
                .then_some(&self.serving_table_vnode_mapping),
        }
    }

    pub(crate) fn is_pinned(&self, table: &TableId) -> bool {
        self.pinned_table_ids.contains(table)
    }

    pub(crate) fn pinned_tables(&self) -> &HashSet<TableId> {
        &self.pinned_table_ids
    }

    pub(crate) fn replace_policy_snapshot(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
        pinned: HashSet<TableId>,
    ) -> HashSet<TableId> {
        let changed = self
            .pinned_table_ids
            .symmetric_difference(&pinned)
            .copied()
            .collect();
        self.table_cache_refill_policies = policies;
        self.pinned_table_ids = pinned;
        changed
    }

    // Pin admission is table-scoped. Changes within a nonempty local vnode set do not
    // invalidate downloads; Foyer still consumes the full updated mappings.
    pub(crate) fn replace_serving_mapping(
        &mut self,
        mapping: HashMap<TableId, Bitmap>,
    ) -> HashSet<TableId> {
        let previous = std::mem::replace(&mut self.serving_table_vnode_mapping, mapping);
        let before = RefillOwnership {
            streaming: self
                .role
                .for_streaming()
                .then_some(&self.streaming_table_vnode_mapping),
            serving: self.role.for_serving().then_some(&previous),
        };
        self.pinned_table_ids
            .iter()
            .copied()
            .filter(|table| before.has_vnodes(table) != self.ownership().has_vnodes(table))
            .collect()
    }

    pub(crate) fn update_streaming_table_vnodes(
        &mut self,
        table: TableId,
        bitmap: Option<Bitmap>,
    ) -> HashSet<TableId> {
        let was_owned = self.ownership().has_vnodes(&table);
        if let Some(bitmap) = bitmap {
            self.streaming_table_vnode_mapping.insert(table, bitmap);
        } else {
            self.streaming_table_vnode_mapping.remove(&table);
        }
        if self.is_pinned(&table) && was_owned != self.ownership().has_vnodes(&table) {
            [table].into()
        } else {
            HashSet::new()
        }
    }
}
