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

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use futures::Future;
use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::key::{get_epoch, Epoch, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::{
    CompactTask, LevelEntry, LevelType, SstableInfo, SubscribeCompactTasksResponse, VacuumTask,
};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::iterator::{BoxedHummockIterator, ConcatIterator, HummockIterator, MergeIterator};
use super::multi_builder::CapacitySplitTableBuilder;
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::sstable_store::SstableStoreRef;
use super::{
    HummockError, HummockResult, HummockStorage, LocalVersionManager, SSTableBuilder,
    SSTableIterator, Sstable,
};
use crate::hummock::vacuum::Vacuum;
use crate::monitor::StateStoreMetrics;

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage configurations.
    pub options: Arc<StorageConfig>,

    /// Local view on the levels of lsm tree.
    pub local_version_manager: Arc<LocalVersionManager>,

    /// The meta client.
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,

    /// SSTable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub stats: Arc<StateStoreMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,
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

impl Compactor {
    /// Create a new compactor.
    pub fn new(context: Arc<CompactorContext>, compact_task: CompactTask) -> Self {
        Self {
            context,
            compact_task,
        }
    }

    /// For compaction from shared buffer to level 0, this is the only function gets called.
    pub async fn compact_shared_buffer(
        context: Arc<CompactorContext>,
        buffers: Vec<SharedBufferBatch>,
        stats: Arc<StateStoreMetrics>,
    ) -> HummockResult<Vec<Sstable>> {
        let mut start_user_keys: Vec<_> = buffers.iter().map(|m| m.start_user_key()).collect();
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

        // Local memory compaction looks at all key ranges.
        let compact_task = CompactTask {
            input_ssts: vec![],
            splits: splits.into_iter().map(|v| v.into()).collect_vec(),
            watermark: u64::MAX,
            sorted_output_ssts: vec![],
            task_id: 0,
            target_level: 0,
            is_target_ultimate_and_leveling: false,
            metrics: None,
            task_status: false,
        };

        let parallelism = compact_task.splits.len();
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let compactor = Compactor::new(context, compact_task.clone());

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let iter = {
                let iters = buffers
                    .iter()
                    .map(|m| Box::new(m.iter()) as BoxedHummockIterator);
                MergeIterator::new(iters, stats.clone())
            };
            compaction_futures.push(tokio::spawn(async move {
                compactor.compact_key_range(split_index, iter).await
            }));
        }

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        let mut err = None;
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!("Shared Buffer Compaction failed with error: {}", e);
                    err = Some(e);
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        if compact_success {
            let mut level0 = Vec::with_capacity(parallelism);

            for (_, sst) in output_ssts {
                level0.extend(sst);
            }

            Ok(level0)
        } else {
            Err(err.unwrap())
        }
    }

    /// Handle a compaction task and report its status to hummock manager.
    /// Always return `Ok` and let hummock manager handle errors.
    pub async fn compact(context: Arc<CompactorContext>, compact_task: CompactTask) {
        tracing::debug!(
            "Ready to handle compaction task: \n{}",
            compact_task_to_string(compact_task.clone())
        );

        // Number of splits (key ranges) is equal to number of compaction tasks
        let parallelism = compact_task.splits.len();
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let mut compactor = Compactor::new(context, compact_task.clone());

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            compaction_futures.push(tokio::spawn(async move {
                compactor
                    .compact_key_range(split_index, compactor.build_sst_iter().await?)
                    .await
            }));
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
                        "Compaction task {} failed with error: {}",
                        compact_task.task_id,
                        e
                    );
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        // After a compaction is done, mutate the compaction task.
        compactor.compact_done(&output_ssts, compact_success).await;
    }

    /// Fill in the compact task and let hummock manager know the compaction output ssts.
    async fn compact_done(&mut self, output_ssts: &[(usize, Vec<Sstable>)], task_ok: bool) {
        self.compact_task.task_status = task_ok;
        self.compact_task
            .sorted_output_ssts
            .reserve(self.compact_task.splits.len());

        for (_, sst) in output_ssts.iter() {
            // for table in &sub_output {
            //     add_table(
            //         compact_task
            //             .metrics
            //             .as_mut()
            //             .unwrap()
            //             .write
            //             .as_mut()
            //             .unwrap(),
            //         table,
            //     );
            // }

            self.compact_task
                .sorted_output_ssts
                .extend(sst.iter().map(|sst| SstableInfo {
                    id: sst.id,
                    key_range: Some(risingwave_pb::hummock::KeyRange {
                        left: sst.meta.smallest_key.clone(),
                        right: sst.meta.largest_key.clone(),
                        inf: false,
                    }),
                }));
        }

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
    async fn compact_key_range(
        &self,
        split_index: usize,
        iter: MergeIterator<'_>,
    ) -> HummockResult<(usize, Vec<Sstable>)> {
        let split = self.compact_task.splits[split_index].clone();
        let kr = KeyRange {
            left: Bytes::copy_from_slice(split.get_left()),
            right: Bytes::copy_from_slice(split.get_right()),
            inf: split.get_inf(),
        };

        // NOTICE: should be user_key overlap, NOT full_key overlap!
        let mut builder = CapacitySplitTableBuilder::new(|| async {
            let table_id = self
                .context
                .hummock_meta_client
                .get_new_table_id()
                .await
                .map_err(HummockError::meta_error)?;
            let builder = HummockStorage::get_builder(&self.context.options);
            Ok((table_id, builder))
        });

        // Monitor time cost building shared buffer to SSTs.
        let build_l0_sst_timer = if self.context.is_share_buffer_compact {
            Some(self.context.stats.write_build_l0_sst_duration.start_timer())
        } else {
            None
        };
        Compactor::compact_and_build_sst(
            &mut builder,
            kr,
            iter,
            !self.compact_task.is_target_ultimate_and_leveling,
            self.compact_task.watermark,
        )
        .await?;
        if let Some(timer) = build_l0_sst_timer {
            timer.observe_duration();
        }

        // Seal.
        builder.seal_current();

        let mut ssts: Vec<Sstable> = Vec::new();
        ssts.reserve(builder.len());
        // TODO: decide upload concurrency
        for (table_id, data, meta) in builder.finish() {
            let sst = Sstable { id: table_id, meta };
            let len = self
                .context
                .sstable_store
                .put(&sst, data, super::CachePolicy::Fill)
                .await?;

            if self.context.is_share_buffer_compact {
                self.context
                    .stats
                    .shared_buffer_to_sstable_size
                    .observe(len as _);
            } else {
                self.context.stats.compaction_upload_sst_counts.inc();
            }

            ssts.push(sst);
        }

        Ok((split_index, ssts))
    }

    /// Build the merge iterator based on the given input ssts.
    async fn build_sst_iter(&self) -> HummockResult<MergeIterator<'_>> {
        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        for LevelEntry {
            level_idx: _,
            level: opt_level,
            ..
        } in &self.compact_task.input_ssts
        {
            let level = opt_level.as_ref().unwrap();
            let tables = self
                .context
                .local_version_manager
                .pick_few_tables(level.get_table_ids())
                .await?;

            // let read_statistics: &mut TableSetStatistics = if *level_idx ==
            // compact_task.target_level {
            //     compact_task.metrics.as_mut().unwrap().read_level_nplus1.as_mut().unwrap()
            // } else {
            //     compact_task.metrics.as_mut().unwrap().read_level_n.as_mut().unwrap()
            // };
            // for table in &tables {
            //     read_statistics.size_gb += table.meta.estimated_size as f64 / (1024 * 1024 *
            // 1024) as f64;     read_statistics.cnt += 1;
            // }

            match level.get_level_type().unwrap() {
                LevelType::Nonoverlapping => {
                    table_iters.push(Box::new(ConcatIterator::new(
                        tables,
                        self.context.sstable_store.clone(),
                    )));
                }
                LevelType::Overlapping => {
                    table_iters.extend(tables.iter().map(|table| -> Box<dyn HummockIterator> {
                        Box::new(SSTableIterator::new(
                            table.clone(),
                            self.context.sstable_store.clone(),
                        ))
                    }));
                }
            }
        }

        Ok(MergeIterator::new(table_iters, self.context.stats.clone()))
    }

    pub async fn try_vacuum(
        vacuum_task: Option<VacuumTask>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        if let Some(vacuum_task) = vacuum_task {
            tracing::debug!("Try to vacuum SSTs {:?}", vacuum_task.sstable_ids);
            match Vacuum::vacuum(
                sstable_store.clone(),
                vacuum_task,
                hummock_meta_client.clone(),
            )
            .await
            {
                Ok(_) => {
                    tracing::debug!("Finish vacuuming SSTs");
                }
                Err(e) => {
                    tracing::warn!("Failed to vacuum SSTs. {}", e);
                }
            }
        }
    }

    /// The background compaction thread that receives compaction tasks from hummock compaction
    /// manager and runs compaction tasks.
    pub fn start_compactor(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let compactor_context = Arc::new(CompactorContext {
            options,
            local_version_manager,
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats,
            is_share_buffer_compact: false,
        });
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let stream_retry_interval = Duration::from_secs(60);
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            // This outer loop is to recreate stream.
            'start_stream: loop {
                tokio::select! {
                    // Wait for interval.
                    _ = min_interval.tick() => {},
                    // Shutdown compactor.
                    _ = shutdown_rx.recv() => {
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
                            "Subsribing to compaction tasks failed with error: {}. Will retry.",
                            e
                        );
                        continue 'start_stream;
                    }
                };

                // This inner loop is to consume stream.
                loop {
                    let message = tokio::select! {
                        message = stream.message() => {
                            message
                        },
                        // Shutdown compactor
                        _ = shutdown_rx.recv() => {
                            tracing::info!("Compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        // The inner Some is the side effect of generated code.
                        Ok(Some(SubscribeCompactTasksResponse {
                            compact_task,
                            vacuum_task,
                        })) => {
                            if let Some(compact_task) = compact_task {
                                Compactor::compact(compactor_context.clone(), compact_task).await;
                            }

                            Compactor::try_vacuum(
                                vacuum_task,
                                sstable_store.clone(),
                                hummock_meta_client.clone(),
                            )
                            .await;
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

    async fn compact_and_build_sst<B, F>(
        sst_builder: &mut CapacitySplitTableBuilder<B>,
        kr: KeyRange,
        mut iter: MergeIterator<'_>,
        has_user_key_overlap: bool,
        watermark: Epoch,
    ) -> HummockResult<()>
    where
        B: FnMut() -> F,
        F: Future<Output = HummockResult<(u64, SSTableBuilder)>>,
    {
        if !kr.left.is_empty() {
            iter.seek(&kr.left).await?;
        } else {
            iter.rewind().await?;
        }

        let mut skip_key = BytesMut::new();
        let mut last_key = BytesMut::new();

        while iter.is_valid() {
            let iter_key = iter.key();

            if !skip_key.is_empty() {
                if VersionedComparator::same_user_key(iter_key, &skip_key) {
                    iter.next().await?;
                    continue;
                } else {
                    skip_key.clear();
                }
            }

            let is_new_user_key =
                last_key.is_empty() || !VersionedComparator::same_user_key(iter_key, &last_key);

            if is_new_user_key {
                if !kr.right.is_empty()
                    && VersionedComparator::compare_key(iter_key, &kr.right)
                        != std::cmp::Ordering::Less
                {
                    break;
                }

                last_key.clear();
                last_key.extend_from_slice(iter_key);
            }

            let epoch = get_epoch(iter_key);

            // Among keys with same user key, only retain keys which satisfy `epoch` >= `watermark`,
            // and the latest key which satisfies `epoch` < `watermark`
            if epoch < watermark {
                skip_key = BytesMut::from(iter_key);
                if iter.value().is_delete() && !has_user_key_overlap {
                    iter.next().await?;
                    continue;
                }
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
