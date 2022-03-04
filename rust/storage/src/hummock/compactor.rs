use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use futures::Future;
use risingwave_common::error::RwError;
use risingwave_pb::hummock::{
    CompactTask, LevelEntry, LevelType, SstableInfo, SubscribeCompactTasksResponse,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use super::iterator::{ConcatIterator, HummockIterator, MergeIterator};
use super::key::{get_epoch, Epoch, FullKey};
use super::key_range::KeyRange;
use super::multi_builder::CapacitySplitTableBuilder;
use super::sstable_manager::SstableStoreRef;
use super::version_cmp::VersionedComparator;
use super::{
    HummockError, HummockMetaClient, HummockOptions, HummockResult, HummockStorage, HummockValue,
    LocalVersionManager, SSTableBuilder, SSTableIterator,
};
use crate::monitor::StateStoreStats;

#[derive(Clone)]
pub struct SubCompactContext {
    pub options: Arc<HummockOptions>,
    pub local_version_manager: Arc<LocalVersionManager>,
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,
    pub sstable_manager: SstableStoreRef,
    pub stats: Arc<StateStoreStats>,
}

pub struct Compactor;

impl Compactor {
    pub async fn run_compact(
        context: &SubCompactContext,
        compact_task: &mut CompactTask,
    ) -> HummockResult<()> {
        let mut overlapping_tables = vec![];
        let mut non_overlapping_table_seqs = vec![];
        for LevelEntry {
            level: opt_level, ..
        } in &compact_task.input_ssts
        {
            let level = opt_level.as_ref().unwrap();
            let tables = context
                .local_version_manager
                .pick_few_tables(level.get_table_ids())
                .await?;
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
                            context.sstable_manager.clone(),
                        ))
                    })
                    .chain(non_overlapping_table_seqs.iter().map(
                        |tableseq| -> Box<dyn HummockIterator> {
                            Box::new(ConcatIterator::new(
                                tableseq.clone(),
                                context.sstable_manager.clone(),
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

        sub_compact_outputsets.sort_by_key(|(sub_kr_idx, _)| *sub_kr_idx);
        for (_, sub_output) in sub_compact_outputsets {
            compact_task.sorted_output_ssts.extend(sub_output);
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

            if epoch < watermark {
                skip_key = BytesMut::from(iter_key);
                if matches!(iter.value(), HummockValue::Delete) && !has_user_key_overlap {
                    iter.next().await?;
                    continue;
                }
            }

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
        output_sst_infos: &mut Vec<SstableInfo>,
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
        Compactor::compact_and_build_sst(&mut builder, kr, iter, has_user_key_overlap, watermark)
            .await?;

        // Seal table for each split
        builder.seal_current();

        output_sst_infos.reserve(builder.len());
        // TODO: decide upload concurrency
        for (table_id, data, meta) in builder.finish() {
            context.sstable_manager.put(table_id, &meta, data).await?;
            context.stats.compaction_upload_sst_counts.inc();
            let info = SstableInfo {
                id: table_id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: meta.get_smallest_key().to_vec(),
                    right: meta.get_largest_key().to_vec(),
                    inf: false,
                }),
            };
            output_sst_infos.push(info);
        }

        Ok(())
    }

    pub async fn compact(
        context: &SubCompactContext,
        mut compact_task: CompactTask,
    ) -> HummockResult<()> {
        let result = Compactor::run_compact(context, &mut compact_task).await;
        if result.is_err() {
            for _sst_to_delete in &compact_task.sorted_output_ssts {
                // TODO: delete these tables in (S3) storage
                // However, if we request a table_id from hummock storage service every time we
                // generate a table, we would not delete here, or we should notify
                // hummock storage service to delete them.
            }
            compact_task.sorted_output_ssts.clear();
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
        options: Arc<HummockOptions>,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        sstable_manager: SstableStoreRef,
        stats: Arc<StateStoreStats>,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let sub_compact_context = SubCompactContext {
            options,
            local_version_manager,
            hummock_meta_client,
            sstable_manager,
            stats,
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
                        tracing::info!("compactor is shutting down");
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
                        tracing::warn!("failed to subscribe_compact_tasks. {}", RwError::from(e));
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
                            tracing::info!("compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        // The inner Some is the side effect of generated code.
                        Ok(Some(SubscribeCompactTasksResponse {
                            compact_task: Some(compact_task),
                        })) => {
                            if let Err(e) =
                                Compactor::compact(&sub_compact_context, compact_task).await
                            {
                                tracing::warn!("failed to compact. {}", RwError::from(e));
                            }
                        }
                        Err(e) => {
                            tracing::warn!("failed to consume stream. {}", e.message());
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
