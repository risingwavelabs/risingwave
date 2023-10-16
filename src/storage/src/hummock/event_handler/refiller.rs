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

use std::collections::{HashSet, VecDeque};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};

use foyer::common::code::Key;
use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntGauge, Registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::file_cache::preclude::*;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::{
    Block, HummockError, HummockResult, Sstable, SstableBlockIndex, SstableStoreRef, TableHolder,
};
use crate::monitor::StoreLocalStatistic;

pub static GLOBAL_CACHE_REFILL_METRICS: LazyLock<CacheRefillMetrics> =
    LazyLock::new(|| CacheRefillMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct CacheRefillMetrics {
    pub refill_duration: HistogramVec,
    pub refill_total: GenericCounterVec<AtomicU64>,
    pub refill_bytes: GenericCounterVec<AtomicU64>,

    pub data_refill_success_duration: Histogram,
    pub meta_refill_success_duration: Histogram,

    pub data_refill_filtered_total: GenericCounter<AtomicU64>,
    pub data_refill_attempts_total: GenericCounter<AtomicU64>,
    pub data_refill_started_total: GenericCounter<AtomicU64>,
    pub meta_refill_attempts_total: GenericCounter<AtomicU64>,

    pub data_refill_ideal_bytes: GenericCounter<AtomicU64>,
    pub data_refill_success_bytes: GenericCounter<AtomicU64>,

    pub refill_queue_total: IntGauge,
}

impl CacheRefillMetrics {
    pub fn new(registry: &Registry) -> Self {
        let refill_duration = register_histogram_vec_with_registry!(
            "refill_duration",
            "refill duration",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_total = register_int_counter_vec_with_registry!(
            "refill_total",
            "refill total",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_bytes = register_int_counter_vec_with_registry!(
            "refill_bytes",
            "refill bytes",
            &["type", "op"],
            registry,
        )
        .unwrap();

        let data_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();
        let meta_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["meta", "success"])
            .unwrap();

        let data_refill_filtered_total = refill_total
            .get_metric_with_label_values(&["data", "filtered"])
            .unwrap();
        let data_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["data", "attempts"])
            .unwrap();
        let data_refill_started_total = refill_total
            .get_metric_with_label_values(&["data", "started"])
            .unwrap();
        let meta_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["meta", "attempts"])
            .unwrap();

        let data_refill_ideal_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "ideal"])
            .unwrap();
        let data_refill_success_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();

        let refill_queue_total = register_int_gauge_with_registry!(
            "refill_queue_total",
            "refill queue total",
            registry,
        )
        .unwrap();

        Self {
            refill_duration,
            refill_total,
            refill_bytes,

            data_refill_success_duration,
            meta_refill_success_duration,
            data_refill_filtered_total,
            data_refill_attempts_total,
            data_refill_started_total,
            meta_refill_attempts_total,

            data_refill_ideal_bytes,
            data_refill_success_bytes,

            refill_queue_total,
        }
    }
}

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Cache refill timeout.
    pub timeout: Duration,

    /// Data file cache refill levels.
    pub data_refill_levels: HashSet<u32>,

    /// Data file cache refill concurrency.
    pub concurrency: usize,

    /// Data file cache refill unit (blocks).
    pub unit: usize,

    /// Data file cache reill unit threshold.
    ///
    /// Only units whose admit rate > threshold will be refilled.
    pub threshold: f64,
}

struct Item {
    handle: JoinHandle<()>,
    event: CacheRefillerEvent,
}

/// A cache refiller for hummock data.
pub struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    context: CacheRefillContext,
}

impl CacheRefiller {
    pub fn new(config: CacheRefillConfig, sstable_store: SstableStoreRef) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        Self {
            queue: VecDeque::new(),
            context: CacheRefillContext {
                config,
                concurrency,
                sstable_store,
            },
        }
    }

    pub fn start_cache_refill(
        &mut self,
        deltas: Vec<SstDeltaInfo>,
        pinned_version: Arc<PinnedVersion>,
        new_pinned_version: PinnedVersion,
    ) {
        let task = CacheRefillTask {
            deltas,
            context: self.context.clone(),
        };
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let handle = tokio::spawn(task.run());
        let item = Item { handle, event };
        self.queue.push_back(item);
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.add(1);
    }

    pub fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }

    pub fn next_event(&mut self) -> NextCacheRefillerEvent<'_> {
        NextCacheRefillerEvent { refiller: self }
    }
}

pub struct NextCacheRefillerEvent<'a> {
    refiller: &'a mut CacheRefiller,
}

impl<'a> Future for NextCacheRefillerEvent<'a> {
    type Output = CacheRefillerEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let refiller = &mut self.deref_mut().refiller;

        if let Some(item) = refiller.queue.front_mut() {
            ready!(item.handle.poll_unpin(cx)).unwrap();
            let item = refiller.queue.pop_front().unwrap();
            GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.sub(1);
            return Poll::Ready(item.event);
        }
        Poll::Pending
    }
}

pub struct CacheRefillerEvent {
    pub pinned_version: Arc<PinnedVersion>,
    pub new_pinned_version: PinnedVersion,
}

#[derive(Clone)]
struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
}

pub struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    async fn run(self) {
        let tasks = self
            .deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!("meta cache refill error: {:?}", e);
                            return;
                        }
                    };
                    Self::data_cache_refill(&context, delta, holders).await;
                }
            })
            .collect_vec();
        let future = join_all(tasks);

        let _ = tokio::time::timeout(self.context.config.timeout, future).await;
    }

    async fn meta_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let tasks = delta
            .insert_sst_infos
            .iter()
            .map(|info| async {
                let mut stats = StoreLocalStatistic::default();
                GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();

                let now = Instant::now();
                let res = context.sstable_store.sstable(info, &mut stats).await;
                GLOBAL_CACHE_REFILL_METRICS
                    .meta_refill_success_duration
                    .observe(now.elapsed().as_secs_f64());
                res
            })
            .collect_vec();
        let holders = try_join_all(tasks).await?;
        Ok(holders)
    }

    async fn data_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        // return if data file cache is disabled
        let Some(filter) = context.sstable_store.data_recent_filter() else {
            return;
        };

        // return if no data to refill
        if delta.insert_sst_infos.is_empty() || delta.delete_sst_object_ids.is_empty() {
            return;
        }

        // return if filtered
        if !context
            .config
            .data_refill_levels
            .contains(&delta.insert_sst_level)
            || !delta
                .delete_sst_object_ids
                .iter()
                .any(|id| filter.contains(id))
        {
            GLOBAL_CACHE_REFILL_METRICS.data_refill_filtered_total.inc();
            return;
        }

        let mut tasks = vec![];
        for sst_info in &holders {
            let task = async move {
                if let Err(e) = Self::data_file_cache_refill_impl(context, sst_info.value()).await {
                    tracing::warn!("data cache refill error: {:?}", e);
                }
            };
            tasks.push(task);
        }

        join_all(tasks).await;
    }

    async fn data_file_cache_refill_impl(
        context: &CacheRefillContext,
        sst: &Sstable,
    ) -> HummockResult<()> {
        let sstable_store = &context.sstable_store;
        let object_id = sst.id;
        let unit = context.config.unit;
        let threshold = context.config.threshold;

        if let Some(filter) = sstable_store.data_recent_filter() {
            filter.insert(object_id);
        }

        let mut tasks = vec![];

        // unit-level refill:
        //
        // Although file cache receivces item by block, a larger range of data is still recommended to reduce
        // S3 iops and per request base latency waste.
        //
        // To decide which unit to refill, we calculate the ratio that the block of a unit will be received by
        // file cache. If the ratio is higher than a threshold, we fetich and refill the whole unit by block.

        for block_index_start in (0..sst.block_count()).step_by(unit) {
            let block_index_end = std::cmp::min(block_index_start + unit, sst.block_count());

            let (range_first, _) = sst.calculate_block_info(block_index_start);
            let (range_last, _) = sst.calculate_block_info(block_index_end - 1);
            let range = range_first.start..range_last.end;

            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_ideal_bytes
                .inc_by((range.end - range.start) as u64);

            let mut writers = Vec::with_capacity(block_index_end - block_index_start);
            let mut ranges = Vec::with_capacity(block_index_end - block_index_start);
            let mut admits = 0;

            for block_index in block_index_start..block_index_end {
                let (range, uncompressed_capacity) = sst.calculate_block_info(block_index);
                let key = SstableBlockIndex {
                    sst_id: object_id,
                    block_idx: block_index as u64,
                };
                let mut writer = sstable_store
                    .data_file_cache()
                    .writer(key, key.serialized_len() + uncompressed_capacity);

                if writer.judge() {
                    admits += 1;
                }

                writers.push(writer);
                ranges.push(range);
            }

            if admits as f64 / writers.len() as f64 >= threshold {
                let task = async move {
                    GLOBAL_CACHE_REFILL_METRICS.data_refill_attempts_total.inc();

                    let permit = context.concurrency.acquire().await.unwrap();

                    GLOBAL_CACHE_REFILL_METRICS.data_refill_started_total.inc();

                    let timer = GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_success_duration
                        .start_timer();

                    let data = sstable_store
                        .store()
                        .read(&sstable_store.get_sst_data_path(object_id), range.clone())
                        .await?;
                    let mut futures = vec![];
                    for (mut writer, r) in writers.into_iter().zip_eq_fast(ranges) {
                        let offset = r.start - range.start;
                        let len = r.end - r.start;
                        let bytes = data.slice(offset..offset + len);

                        let future = async move {
                            let block = Block::decode(
                                bytes,
                                writer.weight() - writer.key().serialized_len(),
                            )?;
                            let block = Box::new(block);
                            writer.force();
                            let res = writer.finish(block).await.map_err(HummockError::file_cache);
                            if matches!(res, Ok(true)) {
                                GLOBAL_CACHE_REFILL_METRICS
                                    .data_refill_success_bytes
                                    .inc_by(len as u64);
                            }
                            res
                        };
                        futures.push(future);
                    }
                    try_join_all(futures)
                        .await
                        .map_err(HummockError::file_cache)?;

                    drop(permit);
                    drop(timer);

                    Ok::<_, HummockError>(())
                };
                tasks.push(task);
            }
        }

        try_join_all(tasks).await?;

        Ok(())
    }
}
