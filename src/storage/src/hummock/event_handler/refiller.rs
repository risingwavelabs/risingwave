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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut, Range};
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{ready, Context, Poll};
use std::time::{Duration, Instant};

use foyer::common::code::Key;
use foyer::common::range::RangeBoundsExt;
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
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::file_cache::preclude::*;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::{
    CachedBlock, FileCacheCompression, HummockError, HummockResult, Sstable, SstableBlockIndex,
    SstableStoreRef, TableHolder,
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

    fn filter(
        context: &CacheRefillContext,
        ssts: &[TableHolder],
        parent_ssts: &[impl Deref<Target = Box<Sstable>>],
    ) -> BTreeMap<SstableUnit, Vec<SstableBlock>> {
        let mut res = BTreeMap::default();

        let data_file_cache = context.sstable_store.data_file_cache();
        if data_file_cache.is_none() {
            return res;
        }

        let unit = context.config.unit;

        let units = ssts
            .iter()
            .flat_map(|sst| {
                let units = Unit::units(sst, unit);
                (0..units).map(|uidx| Unit::new(sst, unit, uidx))
            })
            .collect_vec();

        // if cfg!(debug_assertions) {
        //     // assert units in asc order
        //     units.iter().tuple_windows().for_each(|(a, b)| {
        //         debug_assert_ne!(
        //             KeyComparator::compare_encoded_full_key(a.largest_key(), b.smallest_key()),
        //             std::cmp::Ordering::Greater
        //         )
        //     });
        // }

        // assert units in asc order
        units.iter().tuple_windows().for_each(|(a, b)| {
            assert_ne!(
                KeyComparator::compare_encoded_full_key(a.largest_key(), b.smallest_key()),
                std::cmp::Ordering::Greater
            )
        });

        for psst in parent_ssts {
            let mut idx = 0;
            let mut pblk = 0;

            while idx < units.len() && pblk < psst.block_count() {
                let uleft = units[idx].smallest_key();
                let uright = units[idx].largest_key();

                let pleft = &psst.meta.block_metas[pblk].smallest_key;
                let pright = if pblk + 1 == psst.block_count() {
                    // `largest_key` can be included or excluded, both are treated as included here
                    &psst.meta.largest_key
                } else {
                    &psst.meta.block_metas[pblk + 1].smallest_key
                };

                if uleft > pright {
                    pblk += 1;
                    continue;
                }
                if pleft > uright {
                    idx += 1;
                    continue;
                }

                // uleft <= pright && uleft >= pleft
                if KeyComparator::compare_encoded_full_key(uleft, pright)
                    != std::cmp::Ordering::Greater
                    && KeyComparator::compare_encoded_full_key(uright, pleft)
                        != std::cmp::Ordering::Less
                {
                    res.entry(SstableUnit {
                        sst_obj_id: units[idx].sst.id,
                        unit,
                        uidx: units[idx].uidx,
                        blks: units[idx].blks.clone(),
                    })
                    .or_default()
                    .push(SstableBlock {
                        sst_obj_id: psst.id,
                        blk_idx: pblk,
                    });
                }

                pblk += 1;
            }
        }

        res
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

        // return if recent filter miss
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

        if delta.insert_sst_level == 0 {
            Self::data_file_cache_refill_l0_impl(context, delta, holders).await;
        } else {
            Self::data_file_cache_impl(context, delta, holders).await;
        }
    }

    async fn data_file_cache_refill_l0_impl(
        context: &CacheRefillContext,
        _delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let unit = context.config.unit;

        let mut futures = vec![];

        for sst in &holders {
            for (uidx, blk_start) in (0..sst.block_count()).step_by(unit).enumerate() {
                let blk_end = std::cmp::min(sst.block_count(), blk_start + unit);
                let unit = SstableUnit {
                    sst_obj_id: sst.id,
                    unit,
                    uidx,
                    blks: blk_start..blk_end,
                };
                futures.push(
                    async move { Self::data_file_cache_refill_unit(context, sst, unit).await },
                );
            }
        }
        join_all(futures).await;
    }

    async fn data_file_cache_impl(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let sstable_store = context.sstable_store.clone();
        let futures = delta.delete_sst_object_ids.iter().map(|sst_obj_id| {
            let store = &sstable_store;
            async move { store.sstable_cached(*sst_obj_id).await }
        });
        let parent_ssts = match try_join_all(futures).await {
            Ok(parent_ssts) => parent_ssts.into_iter().flatten().collect_vec(),
            Err(e) => return tracing::error!("get old meta from cache error: {}", e),
        };
        let units = Self::filter(context, &holders, &parent_ssts);
        let ssts: HashMap<HummockSstableObjectId, TableHolder> =
            holders.into_iter().map(|meta| (meta.id, meta)).collect();
        let futures = units.into_iter().map(|(unit, pblks)| {
            let store = &sstable_store;
            let ssts = &ssts;
            async move {
                let refill = pblks
                    .into_iter()
                    .flat_map(|pblk| {
                        store.data_file_cache().exists(&SstableBlockIndex {
                            sst_id: pblk.sst_obj_id,
                            block_idx: pblk.blk_idx as u64,
                        })
                    })
                    .any(|exists| exists);
                let sst = ssts.get(&unit.sst_obj_id).unwrap();

                if refill && let Err(e) = Self::data_file_cache_refill_unit(context, sst, unit).await {
                    tracing::error!("data file cache unit refill error: {}", e);
                }
            }
        });
        join_all(futures).await;
    }

    async fn data_file_cache_refill_unit(
        context: &CacheRefillContext,
        sst: &Sstable,
        unit: SstableUnit,
    ) -> HummockResult<()> {
        let sstable_store = &context.sstable_store;
        let object_id = sst.id;
        let threshold = context.config.threshold;

        if let Some(filter) = sstable_store.data_recent_filter() {
            filter.insert(object_id);
        }

        let blocks = unit.blks.size().unwrap();

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_ideal_bytes
            .inc_by(blocks as u64);

        let mut tasks = vec![];
        let mut writers = Vec::with_capacity(blocks);
        let mut ranges = Vec::with_capacity(blocks);
        let mut admits = 0;

        let (range_first, _) = sst.calculate_block_info(unit.blks.start);
        let (range_last, _) = sst.calculate_block_info(unit.blks.end - 1);
        let range = range_first.start..range_last.end;

        for blk in unit.blks {
            let (range, _uncompressed_capacity) = sst.calculate_block_info(blk);
            let key = SstableBlockIndex {
                sst_id: object_id,
                block_idx: blk as u64,
            };
            // see `CachedBlock::serialized_len()`
            let mut writer = sstable_store
                .data_file_cache()
                .writer(key, key.serialized_len() + 1 + 8 + range.size().unwrap());

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
                        let value = CachedBlock::Fetched {
                            bytes,
                            uncompressed_capacity: writer.weight() - writer.key().serialized_len(),
                        };
                        writer.force();
                        // TODO(MrCroxx): compress if raw is not compressed?
                        // skip compression for it may already be compressed.
                        writer.set_compression(FileCacheCompression::None);
                        let res = writer.finish(value).await.map_err(HummockError::file_cache);
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

        try_join_all(tasks).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct SstableBlock {
    pub sst_obj_id: HummockSstableObjectId,
    pub blk_idx: usize,
}

#[derive(Debug)]
pub struct SstableUnit {
    pub sst_obj_id: HummockSstableObjectId,
    pub unit: usize,
    pub uidx: usize,
    pub blks: Range<usize>,
}

impl PartialEq for SstableUnit {
    fn eq(&self, other: &Self) -> bool {
        self.sst_obj_id == other.sst_obj_id && self.unit == other.unit
    }
}

impl Eq for SstableUnit {}

impl Ord for SstableUnit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.sst_obj_id.cmp(&other.sst_obj_id) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.unit.cmp(&other.unit)
    }
}

impl PartialOrd for SstableUnit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct Unit<'a> {
    sst: &'a Sstable,
    _unit: usize,
    uidx: usize,
    blks: Range<usize>,
}

impl<'a> Unit<'a> {
    fn new(sst: &'a Sstable, unit: usize, uidx: usize) -> Self {
        let blks = unit * uidx..std::cmp::min(unit * (uidx + 1), sst.block_count());
        Self {
            sst,
            _unit: unit,
            uidx,
            blks,
        }
    }

    fn smallest_key(&self) -> &Vec<u8> {
        &self.sst.meta.block_metas[self.blks.start].smallest_key
    }

    // `largest_key` can be included or excluded, both are treated as included here
    fn largest_key(&self) -> &Vec<u8> {
        if self.blks.end == self.sst.block_count() {
            &self.sst.meta.largest_key
        } else {
            &self.sst.meta.block_metas[self.blks.end].smallest_key
        }
    }

    fn units(sst: &Sstable, unit: usize) -> usize {
        sst.block_count() / unit + if sst.block_count() % unit == 0 { 0 } else { 1 }
    }
}
