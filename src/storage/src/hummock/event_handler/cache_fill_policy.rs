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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::future::try_join_all;
use minstant::Instant;
use risingwave_hummock_sdk::KeyComparator;
use risingwave_pb::hummock::{group_delta, HummockVersionDelta};

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::{Sstable, SstableBlockIterator};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

pub struct CacheFillPolicy {
    sstable_store: SstableStoreRef,
    metrics: Arc<CompactorMetrics>,
    cache_refill_io_count_limit: usize,
    pending_preload: AtomicBool,
}

impl CacheFillPolicy {
    pub fn new(
        sstable_store: SstableStoreRef,
        metrics: Arc<CompactorMetrics>,
        cache_refill_io_count_limit: usize,
    ) -> Self {
        Self {
            sstable_store,
            metrics,
            pending_preload: AtomicBool::new(false),
            cache_refill_io_count_limit,
        }
    }

    /// Only allow one thread call this method.
    pub fn execute(self: &Arc<Self>, delta: HummockVersionDelta) {
        if !self.pending_preload.load(Ordering::Acquire) {
            self.pending_preload.store(true, Ordering::Release);
            let policy = self.clone();
            tokio::spawn(async move {
                let start_time = Instant::now();
                let _ = policy.fill_cache(delta).await;
                policy.pending_preload.store(false, Ordering::Release);
                policy
                    .metrics
                    .apply_version_duration
                    .observe(start_time.elapsed().as_secs_f64());
            });
        }
    }

    async fn fill_cache(&self, delta: HummockVersionDelta) -> crate::hummock::HummockResult<()> {
        let stats = StoreLocalStatistic::default();
        let mut prefetch_blocks = vec![];
        for group_delta in delta.group_deltas.values() {
            let mut min_removed_level_idx = u32::MAX;
            let mut flatten_reqs = vec![];
            let mut removed_table_ids = vec![];
            for d in &group_delta.group_deltas {
                if let Some(group_delta::DeltaType::IntraLevel(level_delta)) = d.delta_type.as_ref()
                {
                    for sst in &level_delta.inserted_table_infos {
                        flatten_reqs.push(self.sstable_store.sstable_syncable(sst, &stats));
                    }
                    if !level_delta.removed_table_ids.is_empty() {
                        min_removed_level_idx =
                            std::cmp::min(min_removed_level_idx, level_delta.level_idx);
                    }
                    removed_table_ids.extend(level_delta.removed_table_ids.clone());
                }
            }

            if flatten_reqs.is_empty()
                || min_removed_level_idx != 0
                || self.cache_refill_io_count_limit == 0
            {
                continue;
            }
            let flatten_resp = futures::future::try_join_all(flatten_reqs).await?;
            let mut sstables: Vec<TableHolder> = Vec::with_capacity(flatten_resp.len());
            let mut trivial_move = false;
            for (sst, cache_miss) in flatten_resp {
                if cache_miss == 0 {
                    trivial_move = true;
                }
                sstables.push(sst);
            }
            if trivial_move {
                continue;
            }
            sstables.sort_by(|a, b| {
                KeyComparator::compare_encoded_full_key(
                    &a.value().meta.smallest_key,
                    &b.value().meta.smallest_key,
                )
            });
            for idx in 1..sstables.len() {
                if !KeyComparator::encoded_full_key_less_than(
                    &sstables[idx - 1].value().meta.largest_key,
                    &sstables[idx].value().meta.smallest_key,
                ) {
                    return Ok(());
                }
            }
            let mut group_blocks = vec![];
            for remove_sst_id in &removed_table_ids {
                if sstables.iter().any(|sst| sst.value().id == *remove_sst_id) {
                    continue;
                }
                if let Some(sst) = self.sstable_store.lookup_sstable(*remove_sst_id) {
                    group_blocks.extend(self.refill_sstable(sst.value(), &sstables));
                }
            }
            if !group_blocks.is_empty() {
                prefetch_blocks.extend(group_blocks);
            }
        }
        if prefetch_blocks.is_empty() {
            return Ok(());
        }
        prefetch_blocks.sort();
        prefetch_blocks.dedup();
        let mut last_sst_id = 0;
        let mut start_block_idx = 0;
        let mut end_block_idx = 0;
        let mut prefetch_requests = vec![];
        let mut last_table: Option<TableHolder> = None;
        let mut total_preload_block = 0;
        for (sst_id, block_idx) in prefetch_blocks {
            if total_preload_block > self.cache_refill_io_count_limit {
                break;
            }
            if sst_id != last_sst_id || block_idx > end_block_idx {
                if end_block_idx > start_block_idx {
                    if !last_table
                        .as_ref()
                        .map(|sst| sst.value().id == last_sst_id)
                        .unwrap_or(false)
                    {
                        last_table = self.sstable_store.lookup_sstable(last_sst_id);
                    }
                    end_block_idx = std::cmp::min(
                        end_block_idx,
                        start_block_idx + self.cache_refill_io_count_limit,
                    );
                    if let Some(sst) = last_table.as_ref() {
                        if let Some(handle) =
                            self.sstable_store
                                .prefetch(sst.value(), start_block_idx, end_block_idx)
                        {
                            total_preload_block += end_block_idx - start_block_idx;
                            prefetch_requests.push(handle);
                        }
                    }
                }
                last_sst_id = sst_id;
                start_block_idx = block_idx;
            }
            if end_block_idx == 0 {
                start_block_idx = block_idx;
            }
            end_block_idx = block_idx + 1;
        }
        if end_block_idx > start_block_idx && total_preload_block < self.cache_refill_io_count_limit
        {
            if let Some(sst) = self.sstable_store.lookup_sstable(last_sst_id) {
                end_block_idx = std::cmp::min(
                    end_block_idx,
                    start_block_idx + self.cache_refill_io_count_limit,
                );
                if let Some(handle) =
                    self.sstable_store
                        .prefetch(sst.value(), start_block_idx, end_block_idx)
                {
                    total_preload_block += end_block_idx - start_block_idx;
                    prefetch_requests.push(handle);
                }
            }
        }
        self.metrics
            .preload_io_count
            .inc_by(total_preload_block as u64);
        if !prefetch_requests.is_empty() {
            let _ = try_join_all(prefetch_requests).await;
        }
        Ok(())
    }

    fn refill_sstable(&self, remove_sst: &Sstable, sstables: &[TableHolder]) -> Vec<(u64, usize)> {
        let mut start_idx = sstables.partition_point(|sst| {
            KeyComparator::compare_encoded_full_key(
                &sst.value().meta.largest_key,
                &remove_sst.meta.smallest_key,
            ) == std::cmp::Ordering::Less
        });
        let mut requests = vec![];
        if start_idx == sstables.len() {
            return requests;
        }
        let end_idx = sstables.partition_point(|sst| {
            KeyComparator::compare_encoded_full_key(
                &sst.value().meta.smallest_key,
                &remove_sst.meta.largest_key,
            ) == std::cmp::Ordering::Less
        });
        if start_idx >= end_idx {
            return requests;
        }
        let mut remove_block_iter = SstableBlockIterator::new(remove_sst);
        while remove_block_iter.is_valid() {
            if self.sstable_store.is_hot_block(
                remove_block_iter.sstable.id,
                remove_block_iter.current_block_id as u64,
            ) {
                break;
            }
            remove_block_iter.next();
        }
        if !remove_block_iter.is_valid() {
            return requests;
        }
        let mut add_per_delete = 0;
        const MEMORY_AMPLIFICATION: usize = 5;
        while start_idx < end_idx {
            let mut insert_block_iter =
                SstableBlockIterator::new(sstables[start_idx].value().as_ref());
            while insert_block_iter.is_valid() {
                if KeyComparator::compare_encoded_full_key(
                    insert_block_iter.current_block_largest(),
                    remove_block_iter.current_block_smallest(),
                ) != std::cmp::Ordering::Greater
                {
                    insert_block_iter.next();
                    continue;
                }
                let mut exist_in_cache = true;
                loop {
                    if KeyComparator::encoded_full_key_less_than(
                        insert_block_iter.current_block_largest(),
                        remove_block_iter.current_block_largest(),
                    ) && exist_in_cache
                        && add_per_delete < MEMORY_AMPLIFICATION
                    {
                        break;
                    }
                    remove_block_iter.next();
                    if remove_block_iter.is_valid() {
                        exist_in_cache = self.sstable_store.is_hot_block(
                            remove_block_iter.sstable.id,
                            remove_block_iter.current_block_id as u64,
                        );
                        add_per_delete = 0;
                    } else {
                        return requests;
                    }
                }
                // make sure that remove_block_iter.current_block_largest() >
                // insert_block_iter.current_block_smallest()
                if KeyComparator::encoded_full_key_less_than(
                    remove_block_iter.current_block_smallest(),
                    insert_block_iter.current_block_smallest(),
                ) {
                    assert!(insert_block_iter.is_valid());
                    requests.push((
                        insert_block_iter.sstable.id,
                        insert_block_iter.current_block_id,
                    ));
                    if requests.len() >= self.cache_refill_io_count_limit {
                        return requests;
                    }
                    add_per_delete += 1;
                }
                insert_block_iter.next();
            }
            start_idx += 1;
        }
        requests
    }
}
