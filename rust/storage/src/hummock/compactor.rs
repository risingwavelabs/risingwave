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
//
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use futures::Future;
use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_common::error::RwError;
use risingwave_pb::hummock::{
    CompactMetrics, CompactTask, LevelEntry, LevelType, SstableInfo, SubscribeCompactTasksResponse,
    TableSetStatistics,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::iterator::{ConcatIterator, HummockIterator, MergeIterator};
use super::key::{get_epoch, Epoch, FullKey};
use super::key_range::KeyRange;
use super::multi_builder::CapacitySplitTableBuilder;
use super::sstable_store::SstableStoreRef;
use super::version_cmp::VersionedComparator;
use super::{
    HummockError, HummockMetaClient, HummockResult, HummockStorage, HummockValue,
    LocalVersionManager, SSTableBuilder, SSTableIterator, Sstable,
};
use crate::hummock::vacuum::Vacuum;
use crate::monitor::StateStoreMetrics;

#[derive(Clone)]
pub struct SubCompactContext {
    pub options: Arc<StorageConfig>,
    pub local_version_manager: Arc<LocalVersionManager>,
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,
    pub sstable_store: SstableStoreRef,
    pub stats: Arc<StateStoreMetrics>,
    pub is_share_buffer_compact: bool,
}

pub struct Compactor;
impl Compactor {
    pub async fn run_compact(
        context: &SubCompactContext,
        compact_task: &mut CompactTask,
    ) -> HummockResult<()> {
        let mut overlapping_tables = vec![];
        let mut non_overlapping_table_seqs = vec![];
        let target_level = compact_task.target_level;
        let add_table = |accumu: &mut TableSetStatistics, table: &Sstable| {
            accumu.size_gb += table.meta.estimated_size as f64 / (1024 * 1024 * 1024) as f64;
            accumu.cnt += 1;
        };
        let accumulating_readsize =
            |metrics: &mut CompactMetrics, level_idx: u32, tables: &Vec<Arc<Sstable>>| {
                let read_statistics: &mut TableSetStatistics = if level_idx == target_level {
                    metrics.read_level_nplus1.as_mut().unwrap()
                } else {
                    metrics.read_level_n.as_mut().unwrap()
                };
                for table in tables {
                    add_table(read_statistics, table);
                }
            };
        for LevelEntry {
            level_idx,
            level: opt_level,
            ..
        } in &compact_task.input_ssts
        {
            let level = opt_level.as_ref().unwrap();
            let tables = context
                .local_version_manager
                .pick_few_tables(level.get_table_ids())
                .await?;
            accumulating_readsize(compact_task.metrics.as_mut().unwrap(), *level_idx, &tables);
            if level.get_level_type().unwrap() == LevelType::Nonoverlapping {
                non_overlapping_table_seqs.push(tables);
            } else {
                overlapping_tables.extend(tables);
            }
        }

        let num_sub = compact_task.splits.len();
        compact_task.sorted_output_ssts.reserve(num_sub);

        let mut vec_futures = Vec::with_capacity(num_sub);

        for (kr_idx, kr) in compact_task.splits.iter().enumerate() {
            let mut output_needing_vacuum = vec![];

            let iter = MergeIterator::new(
                overlapping_tables
                    .iter()
                    .map(|table| -> Box<dyn HummockIterator> {
                        Box::new(SSTableIterator::new(
                            table.clone(),
                            context.sstable_store.clone(),
                        ))
                    })
                    .chain(non_overlapping_table_seqs.iter().map(
                        |tableseq| -> Box<dyn HummockIterator> {
                            Box::new(ConcatIterator::new(
                                tableseq.clone(),
                                context.sstable_store.clone(),
                            ))
                        },
                    )),
            );

            let context_clone = context.clone();
            let spawn_kr = KeyRange {
                left: Bytes::copy_from_slice(kr.get_left()),
                right: Bytes::copy_from_slice(kr.get_right()),
                inf: kr.get_inf(),
            };
            let is_target_ultimate_and_leveling = compact_task.is_target_ultimate_and_leveling;
            let watermark = compact_task.watermark;

            vec_futures.push(async move {
                tokio::spawn(async move {
                    (
                        Compactor::sub_compact(
                            context_clone,
                            spawn_kr,
                            iter,
                            &mut output_needing_vacuum,
                            is_target_ultimate_and_leveling,
                            watermark,
                        )
                        .await,
                        kr_idx,
                        output_needing_vacuum,
                    )
                })
                .await
            });
        }

        let stream_of_futures = stream::iter(vec_futures);
        let mut buffered = stream_of_futures.buffer_unordered(num_sub);

        let mut sub_compact_outputsets = Vec::with_capacity(num_sub);

        while let Some(tokio_result) = buffered.next().await {
            let (sub_result, sub_kr_idx, sub_output) = tokio_result.unwrap();
            sub_compact_outputsets.push((sub_kr_idx, sub_output));
            sub_result?
        }

        // `sorted_output_ssts` must be sorted by key range
        sub_compact_outputsets.sort_by_key(|(sub_kr_idx, _)| *sub_kr_idx);
        for (_, sub_output) in sub_compact_outputsets {
            for table in &sub_output {
                add_table(
                    compact_task
                        .metrics
                        .as_mut()
                        .unwrap()
                        .write
                        .as_mut()
                        .unwrap(),
                    table,
                );
            }
            compact_task
                .sorted_output_ssts
                .extend(sub_output.iter().map(|sst| SstableInfo {
                    id: sst.id,
                    key_range: Some(risingwave_pb::hummock::KeyRange {
                        left: sst.meta.get_smallest_key().to_vec(),
                        right: sst.meta.get_largest_key().to_vec(),
                        inf: false,
                    }),
                }));
        }

        Ok(())
    }

    pub async fn compact_and_build_sst<B, F>(
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
                if matches!(iter.value(), HummockValue::Delete) && !has_user_key_overlap {
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

    pub async fn sub_compact(
        context: SubCompactContext,
        kr: KeyRange,
        iter: MergeIterator<'_>,
        output_ssts: &mut Vec<Sstable>,
        // TODO: better naming
        is_target_ultimate_and_leveling: bool,
        watermark: Epoch,
    ) -> HummockResult<()> {
        // NOTICE: should be user_key overlap, NOT full_key overlap!
        let mut builder = CapacitySplitTableBuilder::new(|| async {
            let table_id = context.hummock_meta_client.get_new_table_id().await?;
            let builder = HummockStorage::get_builder(&context.options);
            Ok((table_id, builder))
        });

        let has_user_key_overlap = !is_target_ultimate_and_leveling;

        // Monitor time cost building shared buffer to SSTs.
        let build_l0_sst_timer = if context.is_share_buffer_compact {
            Some(context.stats.write_build_l0_sst_time.start_timer())
        } else {
            None
        };
        Compactor::compact_and_build_sst(&mut builder, kr, iter, has_user_key_overlap, watermark)
            .await?;
        if let Some(timer) = build_l0_sst_timer {
            timer.observe_duration();
        }

        // Seal table for each split
        builder.seal_current();

        output_ssts.reserve(builder.len());
        // TODO: decide upload concurrency
        for (table_id, data, meta) in builder.finish() {
            let sst = Sstable { id: table_id, meta };
            let len = context
                .sstable_store
                .put(&sst, data, super::CachePolicy::Fill)
                .await?;

            if context.is_share_buffer_compact {
                context
                    .stats
                    .write_shared_buffer_sync_size
                    .observe(len as _);
            }

            output_ssts.push(sst);
        }

        Ok(())
    }

    pub async fn compact(
        context: &SubCompactContext,
        mut compact_task: CompactTask,
    ) -> HummockResult<()> {
        let result = Compactor::run_compact(context, &mut compact_task).await;
        if let Err(ref e) = result {
            compact_task.sorted_output_ssts.clear();
            tracing::warn!("compactor error: {}", e);
        }

        let is_task_ok = result.is_ok();

        let report_result = context
            .hummock_meta_client
            .report_compaction_task(compact_task, is_task_ok)
            .await;

        report_result?;

        if is_task_ok {
            Ok(())
        } else {
            // FIXME: error message in `result` should not be ignored
            Err(HummockError::object_io_error("compaction failed."))
        }
    }

    pub fn start_compactor(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let sub_compact_context = SubCompactContext {
            options,
            local_version_manager,
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats,
            is_share_buffer_compact: false,
        };
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let stream_retry_interval = Duration::from_secs(60);
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            // This outer loop is to recreate stream
            'start_stream: loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown compactor
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Compactor is shutting down");
                        return;
                    }
                }

                let mut stream = match sub_compact_context
                    .hummock_meta_client
                    .subscribe_compact_tasks()
                    .await
                {
                    Ok(stream) => stream,
                    Err(e) => {
                        tracing::warn!("Failed to subscribe_compact_tasks. {}", RwError::from(e));
                        continue 'start_stream;
                    }
                };

                // This inner loop is to consume stream
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
                                let input_ssts = compact_task
                                    .input_ssts
                                    .iter()
                                    .flat_map(|v| v.level.as_ref().unwrap().table_ids.clone())
                                    .collect_vec();
                                tracing::debug!("Try to compact SSTs {:?}", input_ssts);
                                if let Err(e) =
                                    Compactor::compact(&sub_compact_context, compact_task).await
                                {
                                    tracing::warn!("Failed to compact SSTs. {}", RwError::from(e));
                                }
                                tracing::debug!("Finish compacting SSTs");
                            }
                            if let Some(vacuum_task) = vacuum_task {
                                tracing::debug!("Try to vacuum SSTs {:?}", vacuum_task.sstable_ids);
                                if let Err(e) = Vacuum::vacuum(
                                    sstable_store.clone(),
                                    vacuum_task,
                                    hummock_meta_client.clone(),
                                )
                                .await
                                {
                                    tracing::warn!("Failed to vacuum SSTs. {}", e);
                                }
                                tracing::debug!("Finish vacuuming SSTs");
                            }
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
}
