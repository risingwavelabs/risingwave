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

//! Common SST metadata loading and block locality selection.
//! Backend adapters resolve policy/projection inputs; this module owns no backend state.

use std::time::Instant;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;

use crate::hummock::refill_locality::{block_vnode_range, vnode_range_overlaps_bitmap};
use crate::hummock::{HummockResult, SstableStoreRef, TableHolder};
use crate::monitor::{GLOBAL_CACHE_REFILL_METRICS, StoreLocalStatistic};

pub(super) mod foyer;
pub(super) mod pin;

/// Physical metadata plus the blocks admitted through this request's logical table projections.
/// Both backends consume this result: Foyer groups eligible blocks into units; Pin downloads
/// the physical object only when at least one admitted block exists. An empty bitmap
/// permits metadata caching but creates no data-download work.
pub(super) struct SstLocality {
    pub sst: TableHolder,
    pub blocks: Bitmap,
}

/// Runs the common table/vnode decision after metadata preparation and before download jobs.
pub(super) fn select_local_blocks<'a>(
    sst: TableHolder,
    admits_projection: impl Fn(&TableId) -> bool,
    context_for: impl Fn(&TableId) -> Option<TableLocality<'a>>,
) -> SstLocality {
    let blocks = (0..sst.block_count())
        .map(|index| {
            let table = sst.meta.block_metas[index].table_id();
            admits_projection(&table)
                && context_for(&table).is_some_and(|context| {
                    let range = block_vnode_range(&sst, index);
                    context.allows_vnode_range(range)
                })
        })
        .collect();
    SstLocality { sst, blocks }
}

/// Shared metadata preparation uses the store's metadata cache and the refill limit.
/// Callers retain their own timeout and batching boundaries.
pub(super) async fn load_sst_metadata(
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
