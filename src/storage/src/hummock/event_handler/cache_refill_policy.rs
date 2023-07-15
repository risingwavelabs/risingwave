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

use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::hummock::{group_delta, HummockVersionDelta};

use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{CachePolicy, HummockResult, TableHolder};
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

        for (meta, _level, removed_sst_object_ids) in &args {
            let mut in_data_file_cache = false;
            for id in removed_sst_object_ids {
                if self
                    .sstable_store
                    .data_file_cache_ssts()
                    .read()
                    .get(id)
                    .is_some()
                {
                    in_data_file_cache = true;
                    break;
                }
            }

            if in_data_file_cache {
                for block_index in 0..meta.value().block_count() {
                    let meta = meta.value().clone();
                    let mut stat = StoreLocalStatistic::default();
                    let sstable_store = self.sstable_store.clone();
                    let future = async move {
                        sstable_store
                            .get_block_response(
                                &meta,
                                block_index,
                                CachePolicy::FillFileCache,
                                &mut stat,
                            )
                            .await
                    };
                    futures.push(future);
                }
            }
        }
        let _ = try_join_all(futures).await;

        // TODO(MrCroxx): set timeout

        Ok(())
    }
}
