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

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::hummock::{group_delta, HummockVersionDelta};

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

    pub async fn execute(self: &Arc<Self>, delta: HummockVersionDelta, max_level: u32) {
        if self.max_preload_wait_time_mill > 0 {
            let policy = self.clone();
            let handle = tokio::spawn(async move {
                let timer = policy.metrics.refill_cache_duration.start_timer();
                let mut preload_count = 0;
                let stats = StoreLocalStatistic::default();
                let mut flatten_reqs = Vec::new();
                let mut levels = vec![];
                let mut removed_sst_object_ids = vec![];
                for group_delta in delta.group_deltas.values() {
                    let mut is_bottommost_level = false;
                    let last_pos = flatten_reqs.len();
                    for d in &group_delta.group_deltas {
                        if let Some(group_delta::DeltaType::IntraLevel(level_delta)) =
                            d.delta_type.as_ref()
                        {
                            if level_delta.level_idx >= max_level {
                                is_bottommost_level = true;
                                break;
                            }
                            for sst in &level_delta.inserted_table_infos {
                                flatten_reqs
                                    .push(policy.sstable_store.sstable_syncable(sst, &stats));
                                levels.push(level_delta.level_idx as usize);
                                removed_sst_object_ids
                                    .push(level_delta.removed_table_object_ids.clone());
                            }
                            preload_count += level_delta.inserted_table_infos.len();
                        }
                    }
                    if is_bottommost_level {
                        while flatten_reqs.len() > last_pos {
                            flatten_reqs.pop();
                        }
                    }
                }
                policy.metrics.preload_io_count.inc_by(preload_count as u64);
                let res = try_join_all(flatten_reqs).await;

                if policy.sstable_store.cache_refill_filter().is_some() {
                    tokio::spawn({
                        async move {
                            if let Err(e) = Self::refill_data_file_cache(
                                policy,
                                res,
                                levels,
                                removed_sst_object_ids,
                            )
                            .await
                            {
                                tracing::warn!("fill data file cache error: {:?}", e);
                            }
                        }
                    });
                }
                timer.observe_duration();
            });
            let _ = tokio::time::timeout(
                Duration::from_millis(self.max_preload_wait_time_mill),
                handle,
            )
            .await;
        }
    }

    async fn refill_data_file_cache(
        self: Arc<Self>,
        fetch_meta_results: HummockResult<Vec<(TableHolder, u64, u64)>>,
        sstable_levels: Vec<usize>,
        removed_sst_object_ids: Vec<Vec<u64>>,
    ) -> HummockResult<()> {
        let metas = fetch_meta_results?
            .into_iter()
            .map(|(meta, _, _)| meta)
            .collect_vec();
        let args = metas
            .into_iter()
            .zip_eq_fast(sstable_levels)
            .zip_eq_fast(removed_sst_object_ids)
            .map(|((t0, t1), t2)| (t0, t1, t2))
            .collect_vec();

        let mut futures = vec![];
        let cache_refill_filter = self.sstable_store.cache_refill_filter().as_ref().unwrap();

        for (meta, _level, removed_sst_object_ids) in &args {
            let mut in_data_file_cache = false;
            for id in removed_sst_object_ids {
                if cache_refill_filter.contains(id) {
                    in_data_file_cache = true;
                    break;
                }
            }

            if in_data_file_cache {
                for block_index in 0..meta.value().block_count() {
                    let meta = meta.value().clone();
                    let mut stat = StoreLocalStatistic::default();
                    let sstable_store = self.sstable_store.clone();
                    let metrics = self.metrics.clone();
                    let future = async move {
                        let now = Instant::now();
                        let res = sstable_store
                            .may_fill_data_file_cache(&meta, block_index, &mut stat)
                            .await;
                        match res {
                            Ok(true) => metrics
                                .refill_data_file_cache_duration
                                .with_label_values(&["admitted"])
                                .observe(now.elapsed().as_secs_f64()),
                            Ok(false) => metrics
                                .refill_data_file_cache_duration
                                .with_label_values(&["rejected"])
                                .observe(now.elapsed().as_secs_f64()),

                            _ => {}
                        }
                        res
                    };
                    futures.push(future);
                }
            } else {
                for _ in 0..meta.value().block_count() {
                    self.metrics
                        .refill_data_file_cache_duration
                        .with_label_values(&["filtered"])
                        .observe(0.0);
                }
            }
        }
        let _ = try_join_all(futures).await;

        // TODO(MrCroxx): set timeout

        Ok(())
    }
}

pub struct CacheRefillFilter<K>
where
    K: Eq + Ord,
{
    refresh_interval: Duration,
    inner: RwLock<CacheRefillFilterInner<K>>,
}

struct CacheRefillFilterInner<K>
where
    K: Eq + Ord,
{
    last_refresh: Instant,
    layers: VecDeque<RwLock<BTreeSet<K>>>,
}

impl<K> CacheRefillFilter<K>
where
    K: Eq + Ord,
{
    pub fn new(layers: usize, refresh_interval: Duration) -> Self {
        assert!(layers > 0);
        let layers = (0..layers)
            .map(|_| BTreeSet::new())
            .map(RwLock::new)
            .collect();
        let inner = CacheRefillFilterInner {
            last_refresh: Instant::now(),
            layers,
        };
        let inner = RwLock::new(inner);
        Self {
            refresh_interval,
            inner,
        }
    }

    pub fn insert(&self, key: K) {
        if let Some(mut inner) = self.inner.try_write() {
            if inner.last_refresh.elapsed() > self.refresh_interval {
                inner.layers.pop_front();
                inner.layers.push_back(RwLock::new(BTreeSet::new()));
            }
        }

        let inner = self.inner.read();
        inner.layers.back().unwrap().write().insert(key);
    }

    pub fn contains(&self, key: &K) -> bool {
        let inner = self.inner.read();
        for layer in inner.layers.iter().rev() {
            if layer.read().contains(key) {
                return true;
            }
        }
        false
    }
}
