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

use futures::future::try_join_all;
use risingwave_common::bitmap::Bitmap;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;

use crate::hummock::refill_locality::block_vnode_range;
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
pub(super) fn select_local_blocks(
    sst: TableHolder,
    admits_projection: impl Fn(&TableId) -> bool,
    allows_vnode_range: impl Fn(&TableId, (usize, usize)) -> bool,
) -> SstLocality {
    let blocks = (0..sst.block_count())
        .map(|index| {
            let table = sst.meta.block_metas[index].table_id();
            admits_projection(&table) && allows_vnode_range(&table, block_vnode_range(&sst, index))
        })
        .collect();
    SstLocality { sst, blocks }
}

/// Loads physical metadata while retaining each input's association with its result.
/// Input order and all-or-error preparation are preserved. Each backend chooses its
/// inputs before calling this loader and retains its own timeout boundary.
pub(super) async fn load_sst_metadata<T>(
    store: &SstableStoreRef,
    inputs: impl Iterator<Item = T>,
    sst_info: impl Fn(&T) -> &SstableInfo,
    concurrency: Option<&Semaphore>,
) -> HummockResult<Vec<(T, TableHolder)>> {
    try_join_all(inputs.map(|input| {
        let sst_info = &sst_info;
        async move {
            GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();
            let _permit = match concurrency {
                Some(concurrency) => Some(concurrency.acquire().await.unwrap()),
                None => None,
            };
            let mut stats = StoreLocalStatistic::default();
            let now = Instant::now();
            let result = store.sstable(sst_info(&input), &mut stats).await;
            stats.discard();
            if result.is_ok() {
                GLOBAL_CACHE_REFILL_METRICS
                    .meta_refill_success_duration
                    .observe(now.elapsed().as_secs_f64());
            }
            result.map(|sst| (input, sst))
        }
    }))
    .await
}
