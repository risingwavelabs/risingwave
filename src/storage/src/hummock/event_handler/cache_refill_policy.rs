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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::HummockSstableObjectId;
use tokio::sync::{mpsc, Mutex};

use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockError, HummockResult, TableHolder};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

const REFILL_DATA_FILE_CACHE_CONCURRENCY: usize = 100;
const REFILL_DATA_FILE_CACHE_TIMEOUT: Duration = Duration::from_secs(10);

struct CacheRefillLevel {
    level_idx: u32,
    insert_sst_infos: Vec<TableHolder>,
    delete_sst_object_ids: Vec<HummockSstableObjectId>,
}

pub struct CacheRefillPolicyConfig {
    pub sstable_store: SstableStoreRef,
    pub metrics: Arc<CompactorMetrics>,

    pub max_preload_wait_time_mill: u64,

    pub refill_data_file_cache_levels: HashSet<u32>,
}

pub struct CacheRefillPolicy {
    sstable_store: SstableStoreRef,
    metrics: Arc<CompactorMetrics>,

    max_preload_wait_time_mill: u64,

    refill_data_file_cache_levels: HashSet<u32>,

    concurrency: Arc<Concurrency>,
}

impl CacheRefillPolicy {
    pub fn new(config: CacheRefillPolicyConfig) -> Self {
        Self {
            sstable_store: config.sstable_store,
            metrics: config.metrics,

            max_preload_wait_time_mill: config.max_preload_wait_time_mill,

            refill_data_file_cache_levels: config.refill_data_file_cache_levels,

            concurrency: Arc::new(Concurrency::new(REFILL_DATA_FILE_CACHE_CONCURRENCY)),
        }
    }

    pub async fn execute(self: &Arc<Self>, sst_delta_infos: Vec<SstDeltaInfo>, max_level: u32) {
        if self.max_preload_wait_time_mill > 0 && !sst_delta_infos.is_empty() {
            let policy = self.clone();
            let handle = tokio::spawn(async move {
                let timer = policy.metrics.refill_cache_duration.start_timer();
                let mut preload_count = 0;
                let stats = StoreLocalStatistic::default();

                let mut reqs = vec![];
                let mut level_idxs = vec![];
                let mut delete_sst_object_id_levels = vec![];

                for sst_delta_info in &sst_delta_infos {
                    if sst_delta_info.insert_sst_level >= max_level
                        || sst_delta_info.insert_sst_infos.is_empty()
                    {
                        continue;
                    }

                    let mut level_reqs = vec![];

                    for sst_info in &sst_delta_info.insert_sst_infos {
                        level_reqs.push(policy.sstable_store.sstable_syncable(sst_info, &stats));
                        preload_count += 1;
                    }

                    level_idxs.push(sst_delta_info.insert_sst_level);
                    delete_sst_object_id_levels.push(sst_delta_info.delete_sst_object_ids.clone());
                    reqs.push(level_reqs);
                }

                policy.metrics.preload_io_count.inc_by(preload_count as u64);

                let res = try_join_all(reqs.into_iter().map(try_join_all)).await;

                let cache_refill_levels = match async move {
                    let cache_refill_levels = res?
                        .into_iter()
                        .zip_eq_fast(level_idxs)
                        .zip_eq_fast(delete_sst_object_id_levels)
                        .map(|((vres, level_idx), delete_sst_object_ids)| {
                            let insert_sst_infos = vres
                                .into_iter()
                                .map(|(sst_info, _, _)| sst_info)
                                .collect_vec();
                            CacheRefillLevel {
                                level_idx,
                                insert_sst_infos,
                                delete_sst_object_ids,
                            }
                        })
                        .collect_vec();

                    Ok::<_, HummockError>(cache_refill_levels)
                }
                .await
                {
                    Ok(cache_refill_levels) => cache_refill_levels,
                    Err(e) => {
                        tracing::warn!("fill meta cache error: {:?}", e);
                        return;
                    }
                };

                if !cache_refill_levels.is_empty()
                    && policy.sstable_store.data_file_cache().is_filter_enabled()
                {
                    tokio::spawn({
                        async move {
                            if let Err(e) =
                                Self::refill_data_file_cache(policy, cache_refill_levels).await
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
        cache_refill_levels: Vec<CacheRefillLevel>,
    ) -> HummockResult<()> {
        let mut handles = vec![];
        let filter = self.sstable_store.data_file_cache_refill_filter().unwrap();

        let start = Instant::now();

        for CacheRefillLevel {
            level_idx,
            insert_sst_infos,
            delete_sst_object_ids,
        } in cache_refill_levels
        {
            if insert_sst_infos.is_empty()
                || delete_sst_object_ids.is_empty()
                || !self.refill_data_file_cache_levels.contains(&level_idx)
            {
                continue;
            }

            let blocks = insert_sst_infos
                .iter()
                .map(|meta| meta.value().block_count())
                .sum::<usize>();

            let mut refill = false;
            for id in &delete_sst_object_ids {
                if filter.contains(id) {
                    refill = true;
                    break;
                }
            }

            if refill {
                for sst_info in &insert_sst_infos {
                    for block_index in 0..sst_info.value().block_count() {
                        let concurrency = self.concurrency.clone();
                        let meta = sst_info.value().clone();
                        let mut stat = StoreLocalStatistic::default();
                        let sstable_store = self.sstable_store.clone();
                        let metrics = self.metrics.clone();

                        concurrency.acquire().await;
                        if start.elapsed() > REFILL_DATA_FILE_CACHE_TIMEOUT {
                            self.metrics
                                .refill_data_file_cache_count
                                .with_label_values(&["timeout"])
                                .inc();
                            continue;
                        }

                        let future = async move {
                            let res = sstable_store
                                .may_fill_data_file_cache(&meta, block_index, &mut stat)
                                .await;
                            match res {
                                Ok(true) => {
                                    metrics
                                        .refill_data_file_cache_count
                                        .with_label_values(&["admitted"])
                                        .inc();
                                }
                                Ok(false) => {
                                    metrics
                                        .refill_data_file_cache_count
                                        .with_label_values(&["rejected"])
                                        .inc();
                                }
                                _ => {}
                            }
                            concurrency.release();
                            res
                        };
                        let handle = tokio::spawn(future);
                        handles.push(handle);
                    }
                }
            } else {
                self.metrics
                    .refill_data_file_cache_count
                    .with_label_values(&["filtered"])
                    .inc_by(blocks as f64);
            }
        }

        let _ = try_join_all(handles).await;

        Ok(())
    }
}

pub struct Concurrency {
    tx: mpsc::UnboundedSender<()>,
    rx: Mutex<mpsc::UnboundedReceiver<()>>,
}

impl Concurrency {
    pub fn new(concurrency: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for _ in 0..concurrency {
            tx.send(()).unwrap();
        }
        Self {
            tx,
            rx: Mutex::new(rx),
        }
    }

    pub async fn acquire(&self) {
        self.rx.lock().await.recv().await.unwrap();
    }

    pub fn release(&self) {
        self.tx.send(()).unwrap();
    }
}
