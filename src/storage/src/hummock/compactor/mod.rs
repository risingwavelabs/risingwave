// Copyright 2022 RisingWave Labs
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
mod event_loop_utils;
mod hummock_event_loop;
mod iceberg_compaction;
mod shared_event_loop;

pub mod compactor_runner;
mod context;
pub mod fast_compactor_runner;
mod iterator;
mod shared_buffer_compact;
pub(super) mod task_progress;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use await_tree::{InstrumentAwait, SpanExt};
pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, TtlCompactionFilter,
};
pub use context::{
    CompactionAwaitTreeRegRef, CompactorContext, await_tree_key, new_compaction_await_tree_reg_ref,
};
pub use hummock_event_loop::start_compactor;
pub use iceberg_compaction::start_iceberg_compactor;
pub use iterator::{ConcatSstableIterator, SstableStreamIterator};
use risingwave_hummock_sdk::{HummockCompactionTaskId, LocalSstableInfo};
pub use shared_buffer_compact::{compact, merge_imms_in_memory};
pub use shared_event_loop::start_shared_compactor;

pub use self::compaction_utils::{
    CompactionStatistics, RemoteBuilderFactory, TaskConfig, check_compaction_result,
    check_flush_result,
};
pub use self::task_progress::TaskProgress;
use super::multi_builder::CapacitySplitTableBuilder;
use super::{GetObjectId, HummockResult, SstableBuilderOptions, Xor16FilterBuilder};
use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::compactor::compactor_runner::compact_and_build_sst;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::{
    BlockedXor16FilterBuilder, FilterBuilder, SstableWriterFactory, UnifiedSstableWriterFactory,
};
use crate::monitor::CompactorMetrics;

/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: CompactorContext,
    object_id_getter: Arc<dyn GetObjectId>,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
    get_id_time: Arc<AtomicU64>,
}

pub type CompactOutput = (usize, Vec<LocalSstableInfo>, CompactionStatistics);

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        context: CompactorContext,
        options: SstableBuilderOptions,
        task_config: TaskConfig,
        object_id_getter: Arc<dyn GetObjectId>,
    ) -> Self {
        Self {
            context,
            options,
            task_config,
            get_id_time: Arc::new(AtomicU64::new(0)),
            object_id_getter,
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
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Option<Arc<TaskProgress>>,
        task_id: Option<HummockCompactionTaskId>,
        split_index: Option<usize>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context
                .compactor_metrics
                .write_build_l0_sst_duration
                .start_timer()
        } else {
            self.context
                .compactor_metrics
                .compact_sst_duration
                .start_timer()
        };

        let (split_table_outputs, table_stats_map) = {
            let factory = UnifiedSstableWriterFactory::new(self.context.sstable_store.clone());
            if self.task_config.use_block_based_filter {
                self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    compaction_catalog_agent_ref,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .instrument_await("compact".verbose())
                .await?
            } else {
                self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    compaction_catalog_agent_ref,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .instrument_await("compact".verbose())
                .await?
            }
        };

        compact_timer.observe_duration();

        Self::report_progress(
            self.context.compactor_metrics.clone(),
            task_progress,
            &split_table_outputs,
            self.context.is_share_buffer_compact,
        );

        self.context
            .compactor_metrics
            .get_table_id_total_time_duration
            .observe(self.get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);

        debug_assert!(
            split_table_outputs
                .iter()
                .all(|table_info| table_info.sst_info.table_ids.is_sorted())
        );

        if task_id.is_some() {
            // skip shared buffer compaction
            tracing::info!(
                "Finish Task {:?} split_index {:?} sst count {}",
                task_id,
                split_index,
                split_table_outputs.len()
            );
        }
        Ok((split_table_outputs, table_stats_map))
    }

    pub fn report_progress(
        metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        ssts: &Vec<LocalSstableInfo>,
        is_share_buffer_compact: bool,
    ) {
        for sst_info in ssts {
            let sst_size = sst_info.file_size();
            if let Some(tracker) = &task_progress {
                tracker.inc_ssts_uploaded();
                tracker.dec_num_pending_write_io();
            }
            if is_share_buffer_compact {
                metrics.shared_buffer_to_sstable_size.observe(sst_size as _);
            } else {
                metrics.compaction_upload_sst_counts.inc();
            }
        }
    }

    async fn compact_key_range_impl<F: SstableWriterFactory, B: FilterBuilder>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Option<Arc<TaskProgress>>,
        object_id_getter: Arc<dyn GetObjectId>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        let builder_factory = RemoteBuilderFactory::<F, B> {
            object_id_getter,
            limiter: self.context.memory_limiter.clone(),
            options: self.options.clone(),
            policy: self.task_config.cache_policy,
            remote_rpc_cost: self.get_id_time.clone(),
            compaction_catalog_agent_ref: compaction_catalog_agent_ref.clone(),
            sstable_writer_factory: writer_factory,
            _phantom: PhantomData,
        };

        let mut sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            self.context.compactor_metrics.clone(),
            task_progress.clone(),
            self.task_config.table_vnode_partition.clone(),
            self.context
                .storage_opts
                .compactor_concurrent_uploading_sst_count,
            compaction_catalog_agent_ref,
        );
        let compaction_statistics = compact_and_build_sst(
            &mut sst_builder,
            &self.task_config,
            self.context.compactor_metrics.clone(),
            iter,
            compaction_filter,
        )
        .instrument_await("compact_and_build_sst".verbose())
        .await?;

        let ssts = sst_builder
            .finish()
            .instrument_await("builder_finish".verbose())
            .await?;

        Ok((ssts, compaction_statistics))
    }
}
