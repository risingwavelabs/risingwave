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

//! Shared table policy and role/ownership admission, before backend work is allocated.

use std::collections::HashMap;
use std::time::Instant;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;

use crate::hummock::refill_locality::{
    RefillOwnership, block_vnode_range, vnode_range_overlaps_bitmap,
};
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::{GLOBAL_CACHE_REFILL_METRICS, StoreLocalStatistic};

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

pub(super) fn table_cache_refill_contexts(
    metadata: &TableCacheMetadata,
    table_ids: impl IntoIterator<Item = TableId>,
) -> TableCacheRefillContextMap {
    let ownership = metadata.ownership();
    let mut contexts = HashMap::new();
    for table_id in table_ids {
        if ownership.serving.is_some()
            && ownership.streaming.is_none()
            && !metadata.serving_table_vnode_mapping.contains_key(&table_id)
        {
            continue;
        }
        contexts.entry(table_id).or_insert_with(|| {
            let policy = table_policy(metadata, &table_id);
            TableCacheRefillContext::new(policy, ownership, &table_id)
        });
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

/// Reject before copying task-local bitmaps. Pin uses its independent enablement and
/// the union of both ownership lanes; ordinary refill preserves the legacy policy.
pub(super) fn admits_table(metadata: &TableCacheMetadata, table: &TableId) -> bool {
    let ownership = metadata.ownership();
    if metadata.is_pinned(table) {
        return ownership.has_vnodes(table);
    }
    let policy = table_policy(metadata, table);
    policy.is_unscoped_enabled()
        || (policy.is_streaming_scoped()
            && ownership
                .streaming
                .and_then(|map| map.get(table))
                .is_some_and(Bitmap::any))
        || (policy.is_serving_scoped()
            && ownership
                .serving
                .and_then(|map| map.get(table))
                .is_some_and(Bitmap::any))
}

/// Physical metadata plus the blocks admitted through this request's logical table projections.
/// Both backends consume this result: Foyer groups eligible blocks into units; Pin downloads
/// the physical object only when at least one admitted block exists. An empty bitmap
/// permits metadata caching but creates no data-download work.
pub(super) struct SstLocality {
    pub sst: TableHolder,
    pub blocks: Bitmap,
}

/// Runs the common table/vnode decision after metadata preparation and before download jobs.
/// `serving_only` narrows L0 Foyer inputs; Pin uses both role-projected ownership lanes.
pub(super) fn select_local_blocks<'a>(
    sst: TableHolder,
    admits_projection: impl Fn(&TableId) -> bool,
    context_for: impl Fn(&TableId) -> Option<TableLocality<'a>>,
    serving_only: bool,
) -> SstLocality {
    let blocks = (0..sst.block_count())
        .map(|index| {
            let table = sst.meta.block_metas[index].table_id();
            admits_projection(&table)
                && context_for(&table).is_some_and(|context| {
                    let range = block_vnode_range(&sst, index);
                    if serving_only {
                        (context.policy.is_unscoped_enabled() || context.policy.is_serving_scoped())
                            && context
                                .serving_vnode_bitmap
                                .is_some_and(|bitmap| vnode_range_overlaps_bitmap(range, bitmap))
                    } else {
                        context.allows_vnode_range(range)
                    }
                })
        })
        .collect();
    SstLocality { sst, blocks }
}

/// Shared metadata preparation uses the store's metadata cache and the refill limit.
/// Callers retain their own timeout and batching boundaries.
pub(super) async fn load_sst(
    store: &SstableStoreRef,
    info: &SstableInfo,
    concurrency: Option<&Semaphore>,
) -> HummockResult<TableHolder> {
    GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();
    let _permit = match concurrency {
        Some(concurrency) => Some(concurrency.acquire().await.unwrap()),
        None => None,
    };
    let mut stats = StoreLocalStatistic::default();
    let now = Instant::now();
    let result = store.sstable(info, &mut stats).await;
    stats.discard();
    if result.is_ok() {
        GLOBAL_CACHE_REFILL_METRICS
            .meta_refill_success_duration
            .observe(now.elapsed().as_secs_f64());
    }
    result
}

/// Pin enablement is independent of the ordinary refill policy. Capture one context per
/// surviving table after metadata I/O; borrow under the same guard as download submission.
pub(super) fn pin_refill_contexts(
    metadata: &TableCacheMetadata,
    tables: impl IntoIterator<Item = TableId>,
) -> HashMap<TableId, TableLocality<'_>> {
    let mut contexts = HashMap::new();
    for table in tables {
        if metadata.is_pinned(&table) && metadata.ownership().has_vnodes(&table) {
            contexts.entry(table).or_insert_with(|| TableLocality {
                policy: CacheRefillPolicy::Both,
                streaming_vnode_bitmap: metadata
                    .ownership()
                    .streaming
                    .and_then(|map| map.get(&table)),
                serving_vnode_bitmap: metadata.ownership().serving.and_then(|map| map.get(&table)),
            });
        }
    }
    contexts
}

impl TableCacheRefillContext {
    fn new(policy: CacheRefillPolicy, ownership: RefillOwnership<'_>, table: &TableId) -> Self {
        Self {
            policy,
            streaming_vnode_bitmap: ownership
                .streaming
                .filter(|_| policy.is_streaming_scoped())
                .and_then(|map| map.get(table))
                .cloned(),
            // Enabled is unscoped for normal refill but needs serving locality for L0.
            serving_vnode_bitmap: ownership
                .serving
                .filter(|_| policy.is_serving_scoped() || policy.is_unscoped_enabled())
                .and_then(|map| map.get(table))
                .cloned(),
        }
    }

    pub(super) fn locality(&self) -> TableLocality<'_> {
        TableLocality {
            policy: self.policy,
            streaming_vnode_bitmap: self.streaming_vnode_bitmap.as_ref(),
            serving_vnode_bitmap: self.serving_vnode_bitmap.as_ref(),
        }
    }
}

/// Borrowed admission inputs, from a Foyer snapshot or Pin's guarded CN metadata.
#[derive(Clone, Copy)]
pub(super) struct TableLocality<'a> {
    policy: CacheRefillPolicy,
    streaming_vnode_bitmap: Option<&'a Bitmap>,
    serving_vnode_bitmap: Option<&'a Bitmap>,
}

impl TableLocality<'_> {
    /// Refine shared admission when the caller has a vnode range (e.g. block metadata).
    /// Enabled keeps main's unscoped normal-refill semantics.
    fn allows_vnode_range(&self, range: (usize, usize)) -> bool {
        self.policy.is_unscoped_enabled()
            || (self.policy.is_streaming_scoped()
                && self
                    .streaming_vnode_bitmap
                    .is_some_and(|bitmap| vnode_range_overlaps_bitmap(range, bitmap)))
            || (self.policy.is_serving_scoped()
                && self
                    .serving_vnode_bitmap
                    .is_some_and(|bitmap| vnode_range_overlaps_bitmap(range, bitmap)))
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
pub(super) fn prepare_foyer_refill(
    deltas: &mut [SstDeltaInfo],
    metadata: &TableCacheMetadata,
) -> TableCacheRefillContextMap {
    // Pin and Foyer can share one physical SST. Exclude only Pin table projections.
    for delta in deltas.iter_mut() {
        let insert_only = delta.delete_sst_infos.is_empty();
        delta.insert_sst_infos.retain(|sst| {
            sst.table_ids.iter().any(|table| {
                !metadata.is_pinned(table)
                    && admits_table(metadata, table)
                    // Streaming writers already warm Foyer on L0 commit. This rule must
                    // not affect Pin, which downloads complete objects independently.
                    && (!insert_only || allows_insert_only_refill(metadata, table))
            })
        });
    }
    table_cache_refill_contexts(
        metadata,
        deltas
            .iter()
            .flat_map(|delta| {
                delta
                    .insert_sst_infos
                    .iter()
                    .flat_map(|sst| sst.table_ids.iter().copied())
            })
            .filter(|table| !metadata.is_pinned(table) && admits_table(metadata, table)),
    )
}

fn allows_insert_only_refill(metadata: &TableCacheMetadata, table: &TableId) -> bool {
    let policy = table_policy(metadata, table);
    // Preserve Enabled's legacy metadata refill, even without a serving bitmap.
    // Data blocks still require serving ownership after metadata is available.
    metadata.role.for_serving()
        && (policy.is_unscoped_enabled()
            || (policy.is_serving_scoped()
                && metadata
                    .ownership()
                    .serving
                    .and_then(|map| map.get(table))
                    .is_some_and(Bitmap::any)))
}
