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
                                    sstable_iters.push(SstableBlockIterator::new(sstable));
                                }
                            }
                        }
                    }
                    let iter = MergeSstableBlockIterator::new(sstable_iters);
                    preload_ssts.push((iter, ssts));
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
                for (iter, ssts) in preload_ssts {
                    if let Err(e) = preload_l1_data(iter, ssts, &sstable_store).await {
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

pub struct SstableBlockIterator {
    sstable: TableHolder,
    block_idx: usize,
}

impl SstableBlockIterator {
    pub fn new(sstable: TableHolder) -> Self {
        Self {
            sstable,
            block_idx: 0,
        }
    }

    pub fn seek(&mut self, full_key: &[u8]) {
        self.block_idx = self
            .sstable
            .value()
            .meta
            .block_metas
            .partition_point(|meta| {
                KeyComparator::compare_encoded_full_key(&meta.smallest_key, full_key)
                    != std::cmp::Ordering::Greater
            })
            .saturating_sub(1);
    }

    #[inline(always)]
    pub fn next(&mut self) {
        self.block_idx += 1;
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.block_idx < self.sstable.value().meta.block_metas.len()
    }

    pub fn current_block_smallest(&self) -> &[u8] {
        &self.sstable.value().meta.block_metas[self.block_idx].smallest_key
    }

    pub fn current_block_largest(&self) -> &[u8] {
        if self.block_idx + 1 < self.sstable.value().meta.block_metas.len() {
            &self.sstable.value().meta.block_metas[self.block_idx + 1].smallest_key
        } else {
            &self.sstable.value().meta.largest_key
        }
    }
}

impl PartialEq for SstableBlockIterator {
    fn eq(&self, other: &Self) -> bool {
        self.current_block_smallest() == other.current_block_smallest()
    }
}

impl Eq for SstableBlockIterator {}

impl PartialOrd for SstableBlockIterator {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if !self.is_valid() || !other.is_valid() {
            return None;
        }
        Some(KeyComparator::compare_encoded_full_key(
            self.current_block_smallest(),
            other.current_block_smallest(),
        ))
    }
}

impl Ord for SstableBlockIterator {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

struct MergeSstableBlockIterator {
    heap: BinaryHeap<Reverse<SstableBlockIterator>>,
    unused_iters: Vec<SstableBlockIterator>,
}

impl MergeSstableBlockIterator {
    pub fn new(unused_iters: Vec<SstableBlockIterator>) -> Self {
        Self {
            unused_iters,
            heap: BinaryHeap::default(),
        }
    }

    pub fn seek(&mut self, full_key: &[u8]) {
        self.unused_iters
            .extend(self.heap.drain().map(|iter| iter.0));
        self.heap = self
            .unused_iters
            .drain_filter(|iter| {
                iter.seek(full_key);
                iter.is_valid()
            })
            .map(|iter| Reverse(iter))
            .collect();
    }

    pub fn is_valid(&self) -> bool {
        !self.heap.is_empty()
    }

    pub fn next(&mut self) {
        let mut top = self.heap.peek_mut().unwrap();
        top.0.next();
        if !top.0.is_valid() {
            let iter = PeekMut::pop(top);
            self.unused_iters.push(iter.0);
        }
    }

    pub fn smallest_block(&self) -> &[u8] {
        self.heap.peek().unwrap().0.current_block_smallest()
    }

    pub fn current_block(&self) -> (HummockSstableObjectId, usize) {
        let top = self.heap.peek().unwrap();
        (top.0.sstable.value().id, top.0.block_idx)
    }
}

async fn preload_l1_data(
    mut iter: MergeSstableBlockIterator,
    insert_ssts: Vec<SstableInfo>,
    sstable_store: &SstableStoreRef,
) -> HummockResult<()> {
    let mut stats = StoreLocalStatistic::default();
    let mut local_block_cache: HashMap<(HummockSstableObjectId, usize), bool> = HashMap::default();
    for sst in insert_ssts {
        let sstable = sstable_store.sstable(&sst, &mut stats).await?;
        iter.seek(&sstable.value().meta.smallest_key);
        let mut blocks = vec![false; sstable.value().meta.block_metas.len()];
        let mut insert_iter = SstableBlockIterator::new(sstable);
        let mut hot_block_count = 0;
        while insert_iter.is_valid() && iter.is_valid() {
            let mut need_preload = false;
            while iter.is_valid() {
                if KeyComparator::compare_encoded_full_key(
                    insert_iter.current_block_largest(),
                    iter.smallest_block(),
                ) != std::cmp::Ordering::Greater
                {
                    break;
                }
                let block = iter.current_block();
                if let Some(ret) = local_block_cache.get(&block) {
                    if *ret {
                        need_preload = true;
                    }
                } else {
                    let ret = sstable_store.is_hot_block(block.0, block.1);
                    if ret {
                        need_preload = true;
                    }
                    local_block_cache.insert(block, ret);
                }
                iter.next();
            }
            if need_preload {
                hot_block_count += 1;
                blocks[insert_iter.block_idx] = true;
            }
            insert_iter.next();
        }
        if hot_block_count > 1
            && hot_block_count * 3 > insert_iter.sstable.value().meta.block_metas.len()
        {
            info!(
                "preload sstable-{} because there are {} blocks is hot",
                sst.sst_id, hot_block_count
            );
            let mut block_index = None;
            let mut preload_index = 0;
            for (index, preload) in blocks.iter().enumerate() {
                if *preload {
                    block_index = Some(index);
                    preload_index = index;
                    break;
                }
            }
            let mut preload_iter = sstable_store
                .get_stream(insert_iter.sstable.value().as_ref(), block_index)
                .await?;
            while let Some(block) = preload_iter.next().await? {
                if blocks[preload_index] {
                    sstable_store.insert_block_cache(sst.object_id, preload_index as u64, block);
                }
                preload_index += 1;
                assert_eq!(preload_iter.get_block_index(), preload_index);
            }
        }
    }
    Ok(())
}
