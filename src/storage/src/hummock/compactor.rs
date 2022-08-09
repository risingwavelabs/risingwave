// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use dyn_clone::DynClone;
use futures::future::try_join_all;
use futures::{stream, FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::config::constant::hummock::{
    CompactionFilterFlag, TABLE_OPTION_DUMMY_RETAINTION_SECOND,
};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_hummock_sdk::key::{
    extract_table_id_and_epoch, get_epoch, get_table_id, Epoch, FullKey,
};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{CompactionGroupId, VersionedComparator};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{CompactTask, LevelType, SstableInfo, SubscribeCompactTasksResponse};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::multi_builder::{
    CapacitySplitTableBuilder, SstableBatchUploader, SstableBuilderConsumer,
};
use super::{
    CompressionAlgorithm, HummockResult, InMemWriterBuilder, SstableBuilderOptions,
    SstableWriterBuilder,
};
use crate::hummock::compaction_executor::CompactionExecutor;
use crate::hummock::iterator::{
    ConcatSstableIterator, Forward, HummockIterator, UnorderedMergeIteratorInner,
};
use crate::hummock::multi_builder::{SealedSstableBuilder, TableBuilderFactory};
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::shared_buffer::{build_ordered_merge_iter, UncommittedData};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::state_store::ForwardIter;
use crate::hummock::utils::{can_concat, MemoryLimiter, MemoryTracker};
use crate::hummock::vacuum::Vacuum;
use crate::hummock::{
    CachePolicy, HummockError, SstableBuilder, SstableIdManagerRef, DEFAULT_ENTRY_SIZE,
};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

pub struct RemoteBuilderFactory<B>
where
    B: SstableWriterBuilder,
{
    sstable_id_manager: SstableIdManagerRef,
    limiter: Arc<MemoryLimiter>,
    options: SstableBuilderOptions,
    remote_rpc_cost: Arc<AtomicU64>,
    writer_builder: B,
}

#[async_trait::async_trait]
impl<B> TableBuilderFactory<B> for RemoteBuilderFactory<B>
where
    B: SstableWriterBuilder,
{
    async fn open_builder(&self) -> HummockResult<(MemoryTracker, SstableBuilder<B::Writer>)> {
        // TODO: streaming upload may take up less memory than the SST capacity, and it also
        // depends on cache policy.
        let tracker = self
            .limiter
            .require_memory(
                (self.options.capacity
                    + self.options.block_capacity
                    + self.options.estimate_bloom_filter_capacity) as u64,
            )
            .await
            .unwrap();
        let timer = Instant::now();
        let table_id = self.sstable_id_manager.get_new_sst_id().await?;
        let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
        self.remote_rpc_cost.fetch_add(cost, Ordering::Relaxed);

        let builder = SstableBuilder::new(
            table_id,
            self.writer_builder.build().await?,
            self.options.clone(),
        );
        Ok((tracker, builder))
    }
}

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage configurations.
    pub options: Arc<StorageConfig>,

    /// The meta client.
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,

    /// Sstable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub stats: Arc<StateStoreMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,

    pub compaction_executor: Option<Arc<CompactionExecutor>>,

    pub filter_key_extractor_manager: FilterKeyExtractorManagerRef,

    pub memory_limiter: Arc<MemoryLimiter>,

    pub sstable_id_manager: SstableIdManagerRef,
}

pub trait CompactionFilter: Send + DynClone {
    fn should_delete(&mut self, _: &[u8]) -> bool {
        false
    }
}

dyn_clone::clone_trait_object!(CompactionFilter);

#[derive(Clone)]
pub struct DummyCompactionFilter;

impl CompactionFilter for DummyCompactionFilter {}

#[derive(Clone)]
pub struct StateCleanUpCompactionFilter {
    existing_table_ids: HashSet<u32>,
    last_table: Option<(u32, bool)>,
}

impl StateCleanUpCompactionFilter {
    fn new(table_id_set: HashSet<u32>) -> Self {
        StateCleanUpCompactionFilter {
            existing_table_ids: table_id_set,
            last_table: None,
        }
    }
}

impl CompactionFilter for StateCleanUpCompactionFilter {
    fn should_delete(&mut self, key: &[u8]) -> bool {
        let table_id_option = get_table_id(key);
        match table_id_option {
            None => false,
            Some(table_id) => {
                if let Some((last_table_id, removed)) = self.last_table.as_ref() {
                    if *last_table_id == table_id {
                        return *removed;
                    }
                }
                let removed = !self.existing_table_ids.contains(&table_id);
                self.last_table = Some((table_id, removed));
                removed
            }
        }
    }
}

#[derive(Clone)]
pub struct TTLCompactionFilter {
    table_id_to_ttl: HashMap<u32, u32>,
    expire_epoch: u64,
    last_table_and_ttl: Option<(u32, u64)>,
}

impl CompactionFilter for TTLCompactionFilter {
    fn should_delete(&mut self, key: &[u8]) -> bool {
        pub use risingwave_common::util::epoch::Epoch;
        let (table_id, epoch) = extract_table_id_and_epoch(key);
        match table_id {
            Some(table_id) => {
                if let Some((last_table_id, ttl_mill)) = self.last_table_and_ttl.as_ref() {
                    if *last_table_id == table_id {
                        let min_epoch = Epoch(self.expire_epoch).subtract_ms(*ttl_mill);
                        return Epoch(epoch) <= min_epoch;
                    }
                }
                match self.table_id_to_ttl.get(&table_id) {
                    Some(ttl_second_u32) => {
                        assert!(*ttl_second_u32 != TABLE_OPTION_DUMMY_RETAINTION_SECOND);
                        // default to zero.
                        let ttl_mill = (*ttl_second_u32 * 1000) as u64;
                        let min_epoch = Epoch(self.expire_epoch).subtract_ms(ttl_mill);
                        self.last_table_and_ttl = Some((table_id, ttl_mill));
                        Epoch(epoch) <= min_epoch
                    }
                    None => false,
                }
            }
            None => false,
        }
    }
}

impl TTLCompactionFilter {
    fn new(table_id_to_ttl: HashMap<u32, u32>, expire: u64) -> Self {
        Self {
            table_id_to_ttl,
            expire_epoch: expire,
            last_table_and_ttl: None,
        }
    }
}

#[derive(Default, Clone)]
struct MultiCompactionFilter {
    filter_vec: Vec<Box<dyn CompactionFilter>>,
}

impl CompactionFilter for MultiCompactionFilter {
    fn should_delete(&mut self, key: &[u8]) -> bool {
        self.filter_vec
            .iter_mut()
            .any(|filter| filter.should_delete(key))
    }
}

impl MultiCompactionFilter {
    fn register(&mut self, filter: Box<dyn CompactionFilter>) {
        self.filter_vec.push(filter);
    }
}

#[derive(Clone)]
/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: Arc<CompactorContext>,

    /// A compaction task received from the hummock manager.
    /// When it's local compaction from memory, it uses a locally
    /// constructed compaction task.
    compact_task: CompactTask,
}

pub type CompactOutput = (usize, Vec<SstableInfo>);

impl Compactor {
    /// Create a new compactor.
    pub fn new(context: Arc<CompactorContext>, compact_task: CompactTask) -> Self {
        Self {
            context,
            compact_task,
        }
    }

    /// Flush shared buffer to level0. Resulted SSTs are grouped by compaction group.
    pub async fn compact_shared_buffer_by_compaction_group(
        context: Arc<CompactorContext>,
        payload: UploadTaskPayload,
    ) -> HummockResult<Vec<(CompactionGroupId, SstableInfo)>> {
        let mut grouped_payload: HashMap<CompactionGroupId, UploadTaskPayload> = HashMap::new();
        for uncommitted_list in payload {
            let mut next_inner = HashSet::new();
            for uncommitted in uncommitted_list {
                let compaction_group_id = match &uncommitted {
                    UncommittedData::Sst((compaction_group_id, _)) => *compaction_group_id,
                    UncommittedData::Batch(batch) => batch.compaction_group_id(),
                };
                let group = grouped_payload
                    .entry(compaction_group_id)
                    .or_insert_with(std::vec::Vec::new);
                if !next_inner.contains(&compaction_group_id) {
                    group.push(vec![]);
                    next_inner.insert(compaction_group_id);
                }
                group.last_mut().unwrap().push(uncommitted);
            }
        }

        let mut futures = vec![];
        for (id, group_payload) in grouped_payload {
            let id_copy = id;
            futures.push(
                Compactor::compact_shared_buffer(context.clone(), group_payload).map_ok(
                    move |results| {
                        results
                            .into_iter()
                            .map(move |result| (id_copy, result))
                            .collect_vec()
                    },
                ),
            );
        }
        // Note that the output is reordered compared with input `payload`.
        let result = try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .collect_vec();
        Ok(result)
    }

    /// For compaction from shared buffer to level 0, this is the only function gets called.
    pub async fn compact_shared_buffer(
        context: Arc<CompactorContext>,
        payload: UploadTaskPayload,
    ) -> HummockResult<Vec<SstableInfo>> {
        let mut start_user_keys = payload
            .iter()
            .flat_map(|data_list| data_list.iter().map(UncommittedData::start_user_key))
            .collect_vec();
        start_user_keys.sort();
        start_user_keys.dedup();
        let mut splits = Vec::with_capacity(start_user_keys.len());
        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
        let mut key_split_append = |key_before_last: &Bytes| {
            splits.last_mut().unwrap().right = key_before_last.clone();
            splits.push(KeyRange::new(key_before_last.clone(), Bytes::new()));
        };
        if start_user_keys.len() > 1 {
            let split_num = context.options.share_buffers_sync_parallelism as usize;
            let buffer_per_split = start_user_keys.len() / split_num;
            for i in 1..split_num {
                key_split_append(
                    &FullKey::from_user_key_slice(
                        start_user_keys[i * buffer_per_split],
                        Epoch::MAX,
                    )
                    .into_inner()
                    .into(),
                );
            }
        }

        let existing_table_ids: Vec<u32> = payload
            .iter()
            .flat_map(|data_list| {
                data_list
                    .iter()
                    .flat_map(|uncommitted_data| match uncommitted_data {
                        UncommittedData::Sst(local_sst_info) => local_sst_info.1.table_ids.clone(),

                        UncommittedData::Batch(shared_buffer_write_batch) => {
                            vec![shared_buffer_write_batch.table_id]
                        }
                    })
            })
            .dedup()
            .collect();

        assert!(!existing_table_ids.is_empty());

        // Local memory compaction looks at all key ranges.
        let compact_task = CompactTask {
            input_ssts: vec![],
            splits: splits.into_iter().map(|v| v.into()).collect_vec(),
            watermark: u64::MAX,
            sorted_output_ssts: vec![],
            task_id: 0,
            target_level: 0,
            gc_delete_keys: false,
            task_status: false,
            compaction_group_id: StaticCompactionGroupId::SharedBuffer.into(),
            existing_table_ids,
            target_file_size: context.options.sstable_size_mb as u64 * (1 << 20),
            compression_algorithm: 0,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
            target_sub_level_id: 0,
        };

        let sstable_store = context.sstable_store.clone();
        let stats = context.stats.clone();

        let parallelism = compact_task.splits.len();
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let compactor = Compactor::new(context, compact_task.clone());

        let mut local_stats = StoreLocalStatistic::default();
        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let iter = build_ordered_merge_iter::<ForwardIter>(
                &payload,
                sstable_store.clone(),
                stats.clone(),
                &mut local_stats,
                Arc::new(SstableIteratorReadOptions::default()),
            )
            .await?;
            let compaction_executor = compactor.context.compaction_executor.as_ref().cloned();

            let split_task = async move { compactor.compact_key_range(split_index, iter).await };
            let rx = Compactor::request_execution(compaction_executor, split_task)?;
            compaction_futures.push(rx);
        }
        local_stats.report(stats.as_ref());

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        let mut err = None;
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!("Shared Buffer Compaction failed with error: {:#?}", e);
                    err = Some(e);
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        if compact_success {
            let mut level0 = Vec::with_capacity(parallelism);

            for (_, ssts) in output_ssts {
                for sst_info in &ssts {
                    compactor
                        .context
                        .stats
                        .write_build_l0_bytes
                        .inc_by(sst_info.file_size as u64);
                }
                level0.extend(ssts);
            }

            Ok(level0)
        } else {
            Err(err.unwrap())
        }
    }

    /// Tries to schedule on `compaction_executor` if `compaction_executor` is not None.
    ///
    /// Tries to schedule on current runtime if `compaction_executor` is None.
    fn request_execution(
        compaction_executor: Option<Arc<CompactionExecutor>>,
        split_task: impl Future<Output = HummockResult<CompactOutput>> + Send + 'static,
    ) -> HummockResult<JoinHandle<HummockResult<CompactOutput>>> {
        match compaction_executor {
            None => Ok(tokio::spawn(split_task)),
            Some(compaction_executor) => {
                let rx = compaction_executor
                    .send_request(split_task)
                    .map_err(HummockError::compaction_executor)?;
                Ok(tokio::spawn(async move {
                    match rx.await {
                        Ok(result) => result,
                        Err(err) => Err(HummockError::compaction_executor(err)),
                    }
                }))
            }
        }
    }

    /// Handles a compaction task and reports its status to hummock manager.
    /// Always return `Ok` and let hummock manager handle errors.
    pub async fn compact(context: Arc<CompactorContext>, compact_task: CompactTask) -> bool {
        use risingwave_common::catalog::TableOption;

        // Set a watermark SST id to prevent full GC from accidentally deleting SSTs for in-progress
        // write op. The watermark is invalidated when this method exits.
        let tracker_id = match context.sstable_id_manager.add_watermark_sst_id(None).await {
            Ok(tracker_id) => tracker_id,
            Err(err) => {
                tracing::warn!("Failed to track pending SST id. {:#?}", err);
                return false;
            }
        };
        let sstable_id_manager_clone = context.sstable_id_manager.clone();
        let _guard = scopeguard::guard(
            (tracker_id, sstable_id_manager_clone),
            |(tracker_id, sstable_id_manager)| {
                tokio::spawn(async move {
                    sstable_id_manager.remove_watermark_sst_id(tracker_id).await;
                });
            },
        );

        let group_label = compact_task.compaction_group_id.to_string();
        let cur_level_label = compact_task.input_ssts[0].level_idx.to_string();
        let compaction_read_bytes = compact_task
            .input_ssts
            .iter()
            .filter(|level| level.level_idx != compact_task.target_level)
            .flat_map(|level| level.table_infos.iter())
            .map(|t| t.file_size)
            .sum::<u64>();
        context
            .stats
            .compact_read_current_level
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc_by(compaction_read_bytes);
        context
            .stats
            .compact_read_sstn_current_level
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc_by(compact_task.input_ssts[0].table_infos.len() as u64);
        context
            .stats
            .compact_frequency
            .with_label_values(&[group_label.as_str(), cur_level_label.as_str()])
            .inc();

        if compact_task.input_ssts.len() > 1 {
            let target_input_level = compact_task.input_ssts.last().unwrap();
            let sec_level_read_bytes: u64 = target_input_level
                .table_infos
                .iter()
                .map(|t| t.file_size)
                .sum();
            let next_level_label = target_input_level.level_idx.to_string();
            context
                .stats
                .compact_read_next_level
                .with_label_values(&[group_label.as_str(), next_level_label.as_str()])
                .inc_by(sec_level_read_bytes);
            context
                .stats
                .compact_read_sstn_next_level
                .with_label_values(&[group_label.as_str(), next_level_label.as_str()])
                .inc_by(compact_task.input_ssts[1].table_infos.len() as u64);
        }

        let timer = context
            .stats
            .compact_task_duration
            .with_label_values(&[compact_task.input_ssts[0].level_idx.to_string().as_str()])
            .start_timer();

        let need_quota = estimate_memory_use_for_compaction(&compact_task);
        tracing::info!(
            "Ready to handle compaction task: {} need memory: {}",
            compact_task.task_id,
            need_quota
        );

        // Number of splits (key ranges) is equal to number of compaction tasks
        let parallelism = compact_task.splits.len();
        assert_ne!(parallelism, 0, "splits cannot be empty");
        context.stats.compact_task_pending_num.inc();
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let mut compactor = Compactor::new(context, compact_task.clone());

        let mut multi_filter = MultiCompactionFilter::default();
        let compaction_filter_flag =
            CompactionFilterFlag::from_bits(compact_task.compaction_filter_mask)
                .unwrap_or_default();
        if compaction_filter_flag.contains(CompactionFilterFlag::STATE_CLEAN) {
            let state_clean_up_filter = Box::new(StateCleanUpCompactionFilter::new(
                HashSet::from_iter(compact_task.existing_table_ids),
            ));

            multi_filter.register(state_clean_up_filter);
        }

        if compaction_filter_flag.contains(CompactionFilterFlag::TTL) {
            let id_to_ttl = compact_task
                .table_options
                .iter()
                .filter(|id_to_option| {
                    let table_option: TableOption = id_to_option.1.into();
                    table_option.retention_seconds.is_some()
                })
                .map(|id_to_option| (*id_to_option.0, id_to_option.1.retention_seconds))
                .collect();
            let ttl_filter = Box::new(TTLCompactionFilter::new(
                id_to_ttl,
                compact_task.current_epoch_time,
            ));
            multi_filter.register(ttl_filter);
        }

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let compaction_executor = compactor.context.compaction_executor.as_ref().cloned();
            let filter = multi_filter.clone();
            let split_task = async move {
                let merge_iter = compactor.build_sst_iter()?;
                compactor
                    .compact_key_range_with_filter(split_index, merge_iter, filter)
                    .await
            };
            let rx = match Compactor::request_execution(compaction_executor, split_task) {
                Ok(rx) => rx,
                Err(err) => {
                    tracing::warn!("Failed to schedule compaction execution: {:#?}", err);
                    return false;
                }
            };
            compaction_futures.push(rx);
        }

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!(
                        "Compaction task {} failed with error: {:#?}",
                        compact_task.task_id,
                        e
                    );
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        // After a compaction is done, mutate the compaction task.
        compactor.compact_done(output_ssts, compact_success).await;
        let cost_time = timer.stop_and_record() * 1000.0;
        tracing::info!(
            "Finished compaction task in {:?}ms: \n{}",
            cost_time,
            compact_task_to_string(&compactor.compact_task)
        );
        compactor.context.stats.compact_task_pending_num.dec();
        for level in &compactor.compact_task.input_ssts {
            for table in &level.table_infos {
                compactor.context.sstable_store.delete_cache(table.id);
            }
        }
        compact_success
    }

    /// Fill in the compact task and let hummock manager know the compaction output ssts.
    async fn compact_done(&mut self, output_ssts: Vec<CompactOutput>, task_ok: bool) {
        self.compact_task.task_status = task_ok;
        self.compact_task
            .sorted_output_ssts
            .reserve(self.compact_task.splits.len());
        let mut compaction_write_bytes = 0;
        for (_, ssts) in output_ssts {
            for sst_info in ssts {
                compaction_write_bytes += sst_info.file_size;
                self.compact_task.sorted_output_ssts.push(sst_info);
            }
        }

        let group_label = self.compact_task.compaction_group_id.to_string();
        let level_label = self.compact_task.target_level.to_string();
        self.context
            .stats
            .compact_write_bytes
            .with_label_values(&[group_label.as_str(), level_label.as_str()])
            .inc_by(compaction_write_bytes);
        self.context
            .stats
            .compact_write_sstn
            .with_label_values(&[group_label.as_str(), level_label.as_str()])
            .inc_by(self.compact_task.sorted_output_ssts.len() as u64);

        if let Err(e) = self
            .context
            .hummock_meta_client
            .report_compaction_task(self.compact_task.to_owned())
            .await
        {
            tracing::warn!(
                "Failed to report compaction task: {}, error: {}",
                self.compact_task.task_id,
                e
            );
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    async fn compact_key_range_impl(
        &self,
        split_index: usize,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
    ) -> HummockResult<CompactOutput> {
        let split = self.compact_task.splits[split_index].clone();
        let kr = KeyRange {
            left: Bytes::copy_from_slice(split.get_left()),
            right: Bytes::copy_from_slice(split.get_right()),
            inf: split.get_inf(),
        };

        let get_id_time = Arc::new(AtomicU64::new(0));
        let max_target_file_size = self.context.options.sstable_size_mb as usize * (1 << 20);
        let cache_policy = if self.compact_task.target_level == 0 {
            CachePolicy::Fill
        } else {
            CachePolicy::NotFill
        };
        let mut options: SstableBuilderOptions = self.context.options.as_ref().into();
        options.capacity = std::cmp::min(
            self.compact_task.target_file_size as usize,
            max_target_file_size,
        );
        options.compression_algorithm = match self.compact_task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        // TODO: change after support of streaming upload
        options.estimate_bloom_filter_capacity = self
            .context
            .filter_key_extractor_manager
            .estimate_bloom_filter_size(options.capacity);
        if options.estimate_bloom_filter_capacity == 0 {
            options.estimate_bloom_filter_capacity = options.capacity / DEFAULT_ENTRY_SIZE;
        }
        let builder_factory = RemoteBuilderFactory {
            sstable_id_manager: self.context.sstable_id_manager.clone(),
            limiter: self.context.memory_limiter.clone(),
            writer_builder: InMemWriterBuilder::from(&options),
            options,
            remote_rpc_cost: get_id_time.clone(),
        };

        let builder_consumer =
            SstableBatchUploader::new(self.context.sstable_store.clone(), cache_policy);
        // NOTICE: should be user_key overlap, NOT full_key overlap!
        let mut builder = CapacitySplitTableBuilder::new(builder_factory, builder_consumer);

        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context.stats.write_build_l0_sst_duration.start_timer()
        } else {
            self.context.stats.compact_sst_duration.start_timer()
        };

        Compactor::compact_and_build_sst(
            &mut builder,
            kr,
            iter,
            self.compact_task.gc_delete_keys,
            self.compact_task.watermark,
            compaction_filter,
        )
        .await?;
        let builder_len = builder.len();
        let sealed_builders = builder.finish().await?;
        compact_timer.observe_duration();

        let mut ssts = Vec::with_capacity(builder_len);
        let mut upload_join_handles = vec![];
        for SealedSstableBuilder {
            sst_info,
            upload_join_handle,
            bloom_filter_size,
        } in sealed_builders
        {
            // bloomfilter occuppy per thousand keys
            self.context
                .filter_key_extractor_manager
                .update_bloom_filter_avg_size(sst_info.file_size as usize, bloom_filter_size);
            let sst_size = sst_info.file_size;
            ssts.push(sst_info);
            upload_join_handles.push(upload_join_handle);

            if self.context.is_share_buffer_compact {
                self.context
                    .stats
                    .shared_buffer_to_sstable_size
                    .observe(sst_size as _);
            } else {
                self.context.stats.compaction_upload_sst_counts.inc();
            }
        }

        // Wait for all upload to finish
        try_join_all(upload_join_handles.into_iter().map(|join_handle| {
            join_handle.map(|result| match result {
                Ok(upload_result) => upload_result,
                Err(e) => Err(HummockError::other(format!(
                    "fail to receive from upload join handle: {:?}",
                    e
                ))),
            })
        }))
        .await?;

        self.context
            .stats
            .get_table_id_total_time_duration
            .observe(get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);
        Ok((split_index, ssts))
    }

    async fn compact_key_range(
        &self,
        split_index: usize,
        iter: impl HummockIterator<Direction = Forward>,
    ) -> HummockResult<CompactOutput> {
        let dummy_compaction_filter = DummyCompactionFilter {};
        self.compact_key_range_impl(split_index, iter, dummy_compaction_filter)
            .await
    }

    async fn compact_key_range_with_filter(
        &self,
        split_index: usize,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
    ) -> HummockResult<CompactOutput> {
        self.compact_key_range_impl(split_index, iter, compaction_filter)
            .await
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(&self) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let mut table_iters = Vec::new();
        let read_options = Arc::new(SstableIteratorReadOptions { prefetch: true });

        // TODO: check memory limit
        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }
            // Do not need to filter the table because manager has done it.

            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&level.table_infos.iter().collect_vec()));
                table_iters.push(ConcatSstableIterator::new(
                    level.table_infos.clone(),
                    self.context.sstable_store.clone(),
                    read_options.clone(),
                ));
            } else {
                for table_info in &level.table_infos {
                    table_iters.push(ConcatSstableIterator::new(
                        vec![table_info.clone()],
                        self.context.sstable_store.clone(),
                        read_options.clone(),
                    ));
                }
            }
        }
        Ok(UnorderedMergeIteratorInner::new(
            table_iters,
            self.context.stats.clone(),
        ))
    }

    /// The background compaction thread that receives compaction tasks from hummock compaction
    /// manager and runs compaction tasks.
    #[allow(clippy::too_many_arguments)]
    pub fn start_compactor(
        options: Arc<StorageConfig>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        compaction_executor: Option<Arc<CompactionExecutor>>,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
        memory_limiter: Arc<MemoryLimiter>,
        sstable_id_manager: SstableIdManagerRef,
    ) -> (JoinHandle<()>, Sender<()>) {
        let compactor_context = Arc::new(CompactorContext {
            options,
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats,
            is_share_buffer_compact: false,
            compaction_executor,
            filter_key_extractor_manager,
            memory_limiter,
            sstable_id_manager,
        });
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let stream_retry_interval = Duration::from_secs(60);
        let join_handle = tokio::spawn(async move {
            let process_task = |task, compactor_context, sstable_store, hummock_meta_client| async {
                match task {
                    Task::CompactTask(compact_task) => {
                        Compactor::compact(compactor_context, compact_task).await;
                    }
                    Task::VacuumTask(vacuum_task) => {
                        Vacuum::vacuum(vacuum_task, sstable_store, hummock_meta_client).await;
                    }
                    Task::FullScanTask(full_scan_task) => {
                        Vacuum::full_scan(full_scan_task, sstable_store, hummock_meta_client).await;
                    }
                }
            };
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            // This outer loop is to recreate stream.
            'start_stream: loop {
                tokio::select! {
                    // Wait for interval.
                    _ = min_interval.tick() => {},
                    // Shutdown compactor.
                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return;
                    }
                }

                let mut stream = match compactor_context
                    .hummock_meta_client
                    .subscribe_compact_tasks()
                    .await
                {
                    Ok(stream) => {
                        tracing::debug!("Succeeded subscribe_compact_tasks.");
                        stream
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Subscribing to compaction tasks failed with error: {}. Will retry.",
                            e
                        );
                        continue 'start_stream;
                    }
                };

                // This inner loop is to consume stream.
                'consume_stream: loop {
                    let message = tokio::select! {
                        message = stream.message() => {
                            message
                        },
                        // Shutdown compactor
                        _ = &mut shutdown_rx => {
                            tracing::info!("Compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        // The inner Some is the side effect of generated code.
                        Ok(Some(SubscribeCompactTasksResponse { task })) => {
                            let task = match task {
                                Some(task) => task,
                                None => continue 'consume_stream,
                            };
                            tokio::spawn(process_task(
                                task,
                                compactor_context.clone(),
                                sstable_store.clone(),
                                hummock_meta_client.clone(),
                            ));
                        }
                        Err(e) => {
                            tracing::warn!("Failed to consume stream. {}", e.message());
                            continue 'start_stream;
                        }
                        _ => {
                            // The stream is exhausted
                            continue 'start_stream;
                        }
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    pub async fn compact_and_build_sst<F, B, C>(
        sst_builder: &mut CapacitySplitTableBuilder<F, B, C>,
        kr: KeyRange,
        mut iter: impl HummockIterator<Direction = Forward>,
        gc_delete_keys: bool,
        watermark: Epoch,
        mut compaction_filter: impl CompactionFilter,
    ) -> HummockResult<()>
    where
        F: TableBuilderFactory<B>,
        B: SstableWriterBuilder,
        C: SstableBuilderConsumer<B::Writer>,
    {
        if !kr.left.is_empty() {
            iter.seek(&kr.left).await?;
        } else {
            iter.rewind().await?;
        }

        let mut last_key = BytesMut::new();
        let mut watermark_can_see_last_key = false;

        while iter.is_valid() {
            let iter_key = iter.key();

            let is_new_user_key =
                last_key.is_empty() || !VersionedComparator::same_user_key(iter_key, &last_key);

            let mut drop = false;
            let epoch = get_epoch(iter_key);
            if is_new_user_key {
                if !kr.right.is_empty()
                    && VersionedComparator::compare_key(iter_key, &kr.right)
                        != std::cmp::Ordering::Less
                {
                    break;
                }

                last_key.clear();
                last_key.extend_from_slice(iter_key);
                watermark_can_see_last_key = false;
            }

            // Among keys with same user key, only retain keys which satisfy `epoch` >= `watermark`.
            // If there is no keys whose epoch is equal than `watermark`, keep the latest key which
            // satisfies `epoch` < `watermark`
            // in our design, frontend avoid to access keys which had be deleted, so we dont
            // need to consider the epoch when the compaction_filter match (it
            // means that mv had drop)
            if (epoch <= watermark && gc_delete_keys && iter.value().is_delete())
                || (epoch < watermark && watermark_can_see_last_key)
            {
                drop = true;
            }

            if !drop && compaction_filter.should_delete(iter_key) {
                drop = true;
            }

            if epoch <= watermark {
                watermark_can_see_last_key = true;
            }

            if drop {
                iter.next().await?;
                continue;
            }

            // Don't allow two SSTs to share same user key
            sst_builder
                .add_full_key(FullKey::from_slice(iter_key), iter.value(), is_new_user_key)
                .await?;

            iter.next().await?;
        }
        Ok(())
    }
}

pub fn estimate_memory_use_for_compaction(task: &CompactTask) -> u64 {
    let mut total_memory_size = 0;
    for level in &task.input_ssts {
        if level.level_type == LevelType::Nonoverlapping as i32 {
            if let Some(table) = level.table_infos.first() {
                total_memory_size += table.file_size * task.splits.len() as u64;
            }
        } else {
            for table in &level.table_infos {
                total_memory_size += table.file_size;
            }
        }
    }
    total_memory_size
}
