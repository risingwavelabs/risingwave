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

//! Foyer input adaptation: ordinary policies, immutable snapshots and L0 serving rules.
//! Runtime scheduling, recent/inheritance filtering and downloads stay in `refiller::foyer`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use futures::future::try_join_all;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;

use super::{SstLocality, TableLocality, load_sst_metadata, select_local_blocks};
use crate::hummock::refill_locality::RefillOwnership;
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};

pub type TableCacheRefillContextMap = HashMap<TableId, TableCacheRefillContext>;

/// Immutable policy/ownership inputs captured before asynchronous refill.
#[derive(Clone)]
pub struct TableCacheRefillContext {
    pub streaming_vnode_bitmap: Option<Bitmap>,
    pub serving_vnode_bitmap: Option<Bitmap>,
    pub policy: CacheRefillPolicy,
}

/// A read-only view of CN metadata for monitor/debugging APIs.
#[derive(Clone)]
pub struct TableCacheRefillMonitorSnapshot {
    pub contexts: TableCacheRefillContextMap,
    pub policies: HashMap<TableId, CacheRefillPolicy>,
    pub default_policy: CacheRefillPolicy,
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

pub(in super::super) fn table_cache_refill_contexts(
    metadata: &TableCacheMetadata,
    table_ids: impl IntoIterator<Item = TableId>,
) -> TableCacheRefillContextMap {
    let ownership = metadata.ownership();
    let mut contexts = HashMap::new();
    for table_id in table_ids {
        if let Entry::Vacant(entry) = contexts.entry(table_id)
            && let Some(context) = TableCacheRefillContext::new(
                table_policy(metadata, &table_id),
                ownership,
                &table_id,
            )
        {
            entry.insert(context);
        }
    }
    contexts
}

pub(crate) fn table_cache_refill_monitor_snapshot(
    metadata: &TableCacheMetadata,
) -> TableCacheRefillMonitorSnapshot {
    let tables = metadata
        .table_cache_refill_policies
        .keys()
        .chain(metadata.streaming_table_vnode_mapping.keys())
        .chain(metadata.serving_table_vnode_mapping.keys())
        .copied();
    TableCacheRefillMonitorSnapshot {
        contexts: table_cache_refill_contexts(
            metadata,
            tables.filter(|table| !metadata.is_pinned(table)),
        ),
        policies: metadata.table_cache_refill_policies.clone(),
        default_policy: metadata.default_policy,
        streaming_table_vnode_mapping: metadata.streaming_table_vnode_mapping.clone(),
        serving_table_vnode_mapping: metadata.serving_table_vnode_mapping.clone(),
    }
}

/// Reject metadata work before copying snapshots. Insert-only refill uses the serving
/// lane because streaming writers already warm Foyer. Enabled preserves metadata warming
/// even without a serving bitmap; block admission still requires serving overlap for L0.
fn admits_metadata_refill(
    policy: CacheRefillPolicy,
    ownership: RefillOwnership<'_>,
    table: &TableId,
    insert_only: bool,
) -> bool {
    let serving = ownership.serving.and_then(|map| map.get(table));
    if insert_only {
        return ownership.serving.is_some()
            && (policy.is_unscoped_enabled()
                || (policy.is_serving_scoped() && serving.is_some_and(Bitmap::any)));
    }
    policy.is_unscoped_enabled()
        || (policy.is_streaming_scoped()
            && ownership
                .streaming
                .and_then(|map| map.get(table))
                .is_some_and(Bitmap::any))
        || (policy.is_serving_scoped() && serving.is_some_and(Bitmap::any))
}

impl TableCacheRefillContext {
    fn new(
        policy: CacheRefillPolicy,
        ownership: RefillOwnership<'_>,
        table: &TableId,
    ) -> Option<Self> {
        let serving = ownership.serving.and_then(|map| map.get(table));
        // Pure serving workers keep metadata warming for Enabled, but do not admit data
        // for tables absent from their serving mapping.
        if ownership.serving.is_some() && ownership.streaming.is_none() && serving.is_none() {
            return None;
        }
        Some(Self {
            policy,
            streaming_vnode_bitmap: ownership
                .streaming
                .filter(|_| policy.is_streaming_scoped())
                .and_then(|map| map.get(table))
                .cloned(),
            // Enabled is unscoped for normal refill but needs serving locality for L0.
            serving_vnode_bitmap: serving
                .filter(|_| policy.is_serving_scoped() || policy.is_unscoped_enabled())
                .cloned(),
        })
    }

    pub(in super::super) fn locality(&self) -> TableLocality<'_> {
        TableLocality {
            policy: self.policy,
            streaming_vnode_bitmap: self.streaming_vnode_bitmap.as_ref(),
            serving_vnode_bitmap: self.serving_vnode_bitmap.as_ref(),
        }
    }
}

/// Missing overrides use the configured default, not Disabled.
fn table_policy(metadata: &TableCacheMetadata, table: &TableId) -> CacheRefillPolicy {
    metadata
        .table_cache_refill_policies
        .get(table)
        .copied()
        .unwrap_or(metadata.default_policy)
}

/// Selects Foyer table projections and captures their immutable inputs before backend dispatch.
/// The caller has already submitted Pin from the shared input; these Foyer-specific exclusions
/// must not narrow Pin's candidates. Snapshot copies follow whole-SST rejection.
pub(in super::super) fn select_table_inputs(
    deltas: &mut [SstDeltaInfo],
    metadata: &TableCacheMetadata,
) -> TableCacheRefillContextMap {
    let ownership = metadata.ownership();
    let mut contexts = HashMap::new();
    // Pin and Foyer can share one physical SST. Exclude only Pin table projections.
    for delta in deltas.iter_mut() {
        let insert_only = delta.delete_sst_infos.is_empty();
        delta.insert_sst_infos.retain(|sst| {
            let mut admitted = false;
            for table in &sst.table_ids {
                if metadata.is_pinned(table) {
                    continue;
                }
                let policy = table_policy(metadata, table);
                if !admits_metadata_refill(policy, ownership, table, insert_only) {
                    continue;
                }
                admitted = true;
                if let Entry::Vacant(entry) = contexts.entry(*table)
                    && let Some(context) = TableCacheRefillContext::new(policy, ownership, table)
                {
                    entry.insert(context);
                }
            }
            admitted
        });
    }
    contexts
}

/// Prepares Foyer's physical metadata and locality before data task generation.
/// Metadata warming remains enabled even when backend data refill is disabled.
pub(in super::super) async fn load_metadata_and_select_blocks(
    store: &SstableStoreRef,
    concurrency: Option<&Semaphore>,
    delta: &SstDeltaInfo,
    contexts: &TableCacheRefillContextMap,
    refill_data: bool,
) -> HummockResult<Vec<SstLocality>> {
    let prepared = try_join_all(delta.insert_sst_infos.iter().map(|info| async move {
        let sst = load_sst_metadata(store, info, concurrency).await?;
        Ok::<_, crate::hummock::HummockError>((info, sst))
    }))
    .await?;
    if !refill_data {
        return Ok(Vec::new());
    }
    Ok(prepared
        .into_iter()
        .map(|(info, sst)| select_blocks(sst, info, contexts, delta.delete_sst_infos.is_empty()))
        .collect())
}

/// Applies logical projections and Foyer's L0 serving-only policy before the shared selector.
pub(in super::super) fn select_blocks(
    sst: TableHolder,
    info: &SstableInfo,
    contexts: &TableCacheRefillContextMap,
    insert_only: bool,
) -> SstLocality {
    select_local_blocks(
        sst,
        |table| info.table_ids.binary_search(table).is_ok(),
        |table| {
            contexts.get(table).map(|context| {
                if insert_only {
                    TableLocality {
                        policy: if context.policy.is_unscoped_enabled()
                            || context.policy.is_serving_scoped()
                        {
                            CacheRefillPolicy::Serving
                        } else {
                            CacheRefillPolicy::Disabled
                        },
                        streaming_vnode_bitmap: None,
                        serving_vnode_bitmap: context.serving_vnode_bitmap.as_ref(),
                    }
                } else {
                    context.locality()
                }
            })
        },
    )
}
