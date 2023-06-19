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

use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use risingwave_pb::hummock::{group_delta, HummockVersionDelta, SstableInfo};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, TableHolder};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

pub struct CacheRefillPolicy {
    sstable_store: SstableStoreRef,
    metrics: Arc<CompactorMetrics>,
    max_preload_wait_time_mill: u64,
}

impl CacheRefillPolicy {
    pub fn new(
        sstable_store: SstableStoreRef,
        metrics: Arc<CompactorMetrics>,
        max_preload_wait_time_mill: u64,
    ) -> Self {
        Self {
            sstable_store,
            metrics,
            max_preload_wait_time_mill,
        }
    }

    pub async fn execute(&self, delta: HummockVersionDelta) {
        if self.max_preload_wait_time_mill > 0 {
            let mut flatten_reqs = vec![];
            let mut stats = StoreLocalStatistic::default();
            let mut preload_ssts = vec![];
            for group_delta in delta.group_deltas.values() {
                let mut ssts = vec![];
                let mut hit_count = 0;
                let mut is_l0_compact = false;
                let mut is_base_level_compact = false;
                for d in &group_delta.group_deltas {
                    if let Some(group_delta::DeltaType::IntraLevel(level_delta)) =
                        d.delta_type.as_ref()
                    {
                        if level_delta.level_idx == 0 {
                            is_l0_compact = true;
                        }
                        if !level_delta.inserted_table_infos.is_empty() && level_delta.level_idx > 0
                        {
                            is_base_level_compact = true;
                        }
                        for sst_id in &level_delta.removed_table_ids {
                            if self.sstable_store.is_hot_sstable(sst_id) {
                                hit_count += 1;
                            }
                        }
                        ssts.extend(level_delta.inserted_table_infos.clone());
                    }
                }
                if hit_count > 0 || is_l0_compact {
                    for sst in &ssts {
                        flatten_reqs.push(self.sstable_store.sstable(sst, &mut stats));
                    }
                }

                if is_l0_compact && is_base_level_compact && hit_count > 0 {
                    let mut sstable_iters = vec![];
                    for d in &group_delta.group_deltas {
                        if let Some(group_delta::DeltaType::IntraLevel(level_delta)) =
                            d.delta_type.as_ref()
                        {
                            for sst_id in &level_delta.removed_table_ids {
                                if let Some(sstable) = self.sstable_store.lookup_sstable(sst_id) {
                                    sstable_iters.push(SstableBlocksInfo::new(sstable));
                                }
                            }
                        }
                    }
                    preload_ssts.push((sstable_iters, ssts));
                }
            }

            self.metrics
                .preload_io_count
                .inc_by(flatten_reqs.len() as u64);
            let sstable_store = self.sstable_store.clone();
            let handle: JoinHandle<()> = tokio::spawn(async move {
                if try_join_all(flatten_reqs).await.is_err() {
                    return;
                }
                for (iters, ssts) in preload_ssts {
                    if let Err(e) = preload_l1_data(iters, ssts, &sstable_store).await {
                        warn!("preload data meet error: {:?}", e);
                    }
                }
            });
            let _ = tokio::time::timeout(
                Duration::from_millis(self.max_preload_wait_time_mill),
                handle,
            )
            .await;
        }
    }
}

pub struct SstableBlocksInfo {
    sstable: TableHolder,
    blocks_in_cache: Vec<bool>,
}

impl SstableBlocksInfo {
    pub fn new(sstable: TableHolder) -> Self {
        Self {
            sstable,
            blocks_in_cache: vec![false; sstable.value().meta.block_metas.len()],
        }
    }
}

async fn preload_l1_data(
    mut removed_sstables: Vec<SstableBlocksInfo>,
    insert_ssts: Vec<SstableInfo>,
    sstable_store: &SstableStoreRef,
) -> HummockResult<()> {
    let mut stats = StoreLocalStatistic::default();
    for iter in &mut removed_sstables {
        for idx in 0..iter.blocks_in_cache.len() {
            iter.blocks_in_cache[idx] = sstable_store.is_hot_block(iter.sstable.value().id, idx);
        }
    }

    const MIN_OVERLAP_HOT_BLOCK_COUNT: usize = 4;
    for sst in insert_ssts {
        let key_range = sst.key_range.as_ref().unwrap();
        let mut replace_hot_block = 0;
        for remove_sst in &removed_sstables {
            let start_block_idx =
                remove_sst
                    .sstable
                    .value()
                    .meta
                    .block_metas
                    .partition_point(|meta| {
                        KeyComparator::compare_encoded_full_key(&meta.smallest_key, &key_range.left)
                            != std::cmp::Ordering::Greater
                    });
            let mut end_block_idx =
                remove_sst
                    .sstable
                    .value()
                    .meta
                    .block_metas
                    .partition_point(|meta| {
                        KeyComparator::compare_encoded_full_key(
                            &meta.smallest_key,
                            &key_range.right,
                        ) != Ordering::Greater
                    });
            if end_block_idx > 0
                && KeyComparator::compare_encoded_full_key(
                    &remove_sst.sstable.value().meta.largest_key,
                    &key_range.right,
                ) == Ordering::Greater
            {
                end_block_idx -= 1;
            }
            for idx in start_block_idx..end_block_idx {
                if remove_sst.blocks_in_cache[idx] {
                    replace_hot_block += 1;
                }
            }
        }

        if replace_hot_block < MIN_OVERLAP_HOT_BLOCK_COUNT {
            continue;
        }
        let sstable = sstable_store.sstable(&sst, &mut stats).await?;
        if replace_hot_block < sstable.value().meta.block_metas.len() / 20 {
            info!(
                "skip prefetch for sst-{} hot blocks: {}",
                sst.sst_id, replace_hot_block
            );
            continue;
        }
        let mut smallest_key = sstable.value().meta.largest_key.clone();
        let mut largest_key = sstable.value().meta.smallest_key.clone();
        for info in &removed_sstables {
            let remove_meta = &info.sstable.value().meta;
            for idx in 0..info.blocks_in_cache.len() {
                if info.blocks_in_cache[idx] {
                    if KeyComparator::encoded_full_key_less_than(
                        remove_meta.block_metas[idx].smallest_key.as_ref(),
                        &smallest_key,
                    ) {
                        smallest_key = remove_meta.block_metas[idx].smallest_key.clone();
                    }
                    break;
                }
            }
            for idx in (0..info.blocks_in_cache.len()).rev() {
                if info.blocks_in_cache[idx] {
                    let next_key = if idx + 1 < info.blocks_in_cache.len() {
                        remove_meta.block_metas[idx + 1].smallest_key.as_ref()
                    } else {
                        remove_meta.largest_key.as_ref()
                    };
                    if KeyComparator::encoded_full_key_less_than(&largest_key, next_key) {
                        largest_key = next_key.to_vec();
                    }
                    break;
                }
            }
        }

        if !KeyComparator::encoded_full_key_less_than(&smallest_key, &largest_key) {
            continue;
        }
        let mut start_index = 0;
        let sstable_meta = sstable.value().as_ref();
        for idx in 0..sstable_meta.meta.block_metas.len() {
            if KeyComparator::encoded_full_key_less_than(
                &smallest_key,
                &sstable_meta.meta.block_metas[idx].smallest_key,
            ) {
                break;
            }
            start_index = idx;
        }
        info!(
            "prefetch for sst-{} hot blocks: {}",
            sst.sst_id, replace_hot_block
        );
        let mut preload_iter = sstable_store
            .get_stream(sstable_meta, Some(start_index))
            .await?;
        let mut index = start_index;
        while let Some(block) = preload_iter.next().await? {
            sstable_store.insert_block_cache(sstable_meta.id, index as u64, block);
            index += 1;
            if index >= sstable_meta.meta.block_metas.len()
                || index - start_index > replace_hot_block
                || KeyComparator::encoded_full_key_less_than(
                    &largest_key,
                    &sstable_meta.meta.block_metas[index].smallest_key,
                )
            {
                break;
            }
        }
    }
    Ok(())
}
