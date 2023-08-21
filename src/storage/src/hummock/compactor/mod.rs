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

mod compaction_executor;
mod compaction_filter;
pub mod compaction_utils;
pub mod compactor_runner;
mod context;
mod iterator;
mod shared_buffer_compact;
pub(super) mod task_progress;

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use await_tree::InstrumentAwait;
pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, StateCleanUpCompactionFilter,
    TtlCompactionFilter,
};
pub use context::CompactorContext;
use futures::future::try_join_all;
pub use iterator::{ConcatSstableIterator, SstableStreamIterator};
use risingwave_hummock_sdk::{HummockCompactionTaskId, LocalSstableInfo};
pub use shared_buffer_compact::{compact, merge_imms_in_memory};

pub use self::compaction_utils::{CompactionStatistics, RemoteBuilderFactory, TaskConfig};
pub use self::task_progress::TaskProgress;
use super::multi_builder::CapacitySplitTableBuilder;
use super::{
    CompactionDeleteRanges, HummockResult, MemoryLimiter, SstableBuilderOptions,
    SstableObjectIdManager, SstableObjectIdManagerRef, SstableStoreRef, Xor16FilterBuilder,
};
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::compactor_runner::compact_and_build_sst;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::multi_builder::SplitTableOutput;
use crate::hummock::{
    BatchSstableWriterFactory, BlockedXor16FilterBuilder, FilterBuilder, HummockError,
    SstableWriterFactory, StreamingSstableWriterFactory,
};
use crate::monitor::CompactorMetrics;

/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    compactor_metrics: Arc<CompactorMetrics>,
    is_share_buffer_compact: bool,
    sstable_store: SstableStoreRef,
    memory_limiter: Arc<MemoryLimiter>,

    sstable_object_id_manager: Arc<SstableObjectIdManager>,
    compact_iter_recreate_timeout_ms: u64,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
    get_id_time: Arc<AtomicU64>,
}

pub type CompactOutput = (usize, Vec<LocalSstableInfo>, CompactionStatistics);

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        options: SstableBuilderOptions,
        task_config: TaskConfig,
        compactor_metrics: Arc<CompactorMetrics>,
        is_share_buffer_compact: bool,
        sstable_store: SstableStoreRef,
        memory_limiter: Arc<MemoryLimiter>,

        sstable_object_id_manager: Arc<SstableObjectIdManager>,
        compact_iter_recreate_timeout_ms: u64,
    ) -> Self {
        Self {
            compactor_metrics,
            is_share_buffer_compact,
            sstable_store,
            memory_limiter,

            sstable_object_id_manager,
            options,
            task_config,
            get_id_time: Arc::new(AtomicU64::new(0)),
            compact_iter_recreate_timeout_ms,
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    ///
    /// `task_progress` is only used for tasks on the compactor.
    async fn compact_key_range(
        &self,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        del_agg: Arc<CompactionDeleteRanges>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Option<Arc<TaskProgress>>,
        task_id: Option<HummockCompactionTaskId>,
        split_index: Option<usize>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.is_share_buffer_compact {
            self.compactor_metrics
                .write_build_l0_sst_duration
                .start_timer()
        } else {
            self.compactor_metrics.compact_sst_duration.start_timer()
        };

        let (split_table_outputs, table_stats_map) =
            if self.sstable_store.store().support_streaming_upload() {
                let factory = StreamingSstableWriterFactory::new(self.sstable_store.clone());
                if self.task_config.use_block_based_filter {
                    self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        del_agg,
                        filter_key_extractor,
                        task_progress.clone(),
                    )
                    .verbose_instrument_await("compact")
                    .await?
                } else {
                    self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        del_agg,
                        filter_key_extractor,
                        task_progress.clone(),
                    )
                    .verbose_instrument_await("compact")
                    .await?
                }
            } else {
                let factory = BatchSstableWriterFactory::new(self.sstable_store.clone());
                if self.task_config.use_block_based_filter {
                    self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        del_agg,
                        filter_key_extractor,
                        task_progress.clone(),
                    )
                    .verbose_instrument_await("compact")
                    .await?
                } else {
                    self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                        factory,
                        iter,
                        compaction_filter,
                        del_agg,
                        filter_key_extractor,
                        task_progress.clone(),
                    )
                    .verbose_instrument_await("compact")
                    .await?
                }
            };

        compact_timer.observe_duration();

        let mut ssts = Vec::with_capacity(split_table_outputs.len());
        let mut upload_join_handles = vec![];

        for SplitTableOutput {
            sst_info,
            upload_join_handle,
        } in split_table_outputs
        {
            let sst_size = sst_info.file_size();
            ssts.push(sst_info);

            let tracker_cloned = task_progress.clone();
            let compactor_metrics_cloned = self.compactor_metrics.clone();
            let is_share_buffer_compact = self.is_share_buffer_compact;
            upload_join_handles.push(async move {
                upload_join_handle
                    .verbose_instrument_await("upload")
                    .await
                    .map_err(HummockError::sstable_upload_error)??;
                if let Some(tracker) = tracker_cloned {
                    tracker.inc_ssts_uploaded();
                    tracker
                        .num_pending_write_io
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                }
                if is_share_buffer_compact {
                    compactor_metrics_cloned
                        .shared_buffer_to_sstable_size
                        .observe(sst_size as _);
                } else {
                    compactor_metrics_cloned.compaction_upload_sst_counts.inc();
                }
                Ok::<_, HummockError>(())
            });
        }

        // Check if there are any failed uploads. Report all of those SSTs.
        try_join_all(upload_join_handles)
            .verbose_instrument_await("join")
            .await?;
        self.compactor_metrics
            .get_table_id_total_time_duration
            .observe(self.get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);

        debug_assert!(ssts
            .iter()
            .all(|table_info| table_info.sst_info.get_table_ids().is_sorted()));

        if task_id.is_some() {
            // skip shared buffer compaction
            tracing::info!(
                "Finish Task {:?} split_index {:?} sst count {}",
                task_id,
                split_index,
                ssts.len()
            );
        }
        Ok((ssts, table_stats_map))
    }

    async fn compact_key_range_impl<F: SstableWriterFactory, B: FilterBuilder>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        del_agg: Arc<CompactionDeleteRanges>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Option<Arc<TaskProgress>>,
    ) -> HummockResult<(Vec<SplitTableOutput>, CompactionStatistics)> {
        let builder_factory = RemoteBuilderFactory::<F, B> {
            sstable_object_id_manager: self.sstable_object_id_manager.clone(),
            limiter: self.memory_limiter.clone(),
            options: self.options.clone(),
            policy: self.task_config.cache_policy,
            remote_rpc_cost: self.get_id_time.clone(),
            filter_key_extractor,
            sstable_writer_factory: writer_factory,
            _phantom: PhantomData,
        };

        let mut sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            self.compactor_metrics.clone(),
            task_progress.clone(),
            self.task_config.is_target_l0_or_lbase,
            self.task_config.split_by_table,
            self.task_config.split_weight_by_vnode,
        );
        let compaction_statistics = compact_and_build_sst(
            &mut sst_builder,
            del_agg,
            &self.task_config,
            self.compactor_metrics.clone(),
            iter,
            compaction_filter,
            task_progress,
        )
        .verbose_instrument_await("compact_and_build_sst")
        .await?;

        let ssts = sst_builder
            .finish()
            .verbose_instrument_await("builder_finish")
            .await?;

        Ok((ssts, compaction_statistics))
    }
}
