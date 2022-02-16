use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use risingwave_pb::hummock::{CompactTask, LevelEntry, LevelType, SstableInfo};

use super::iterator::{ConcatIterator, HummockIterator, MergeIterator};
use super::key::{get_epoch, Epoch, FullKey};
use super::key_range::KeyRange;
use super::multi_builder::CapacitySplitTableBuilder;
use super::version_cmp::VersionedComparator;
use super::{
    HummockError, HummockMetaClient, HummockOptions, HummockResult, HummockStorage, HummockValue,
    LocalVersionManager, SSTable, SSTableIterator,
};
use crate::hummock::cloud::gen_remote_sstable;
use crate::object::ObjectStore;

pub struct SubCompactContext {
    pub options: Arc<HummockOptions>,
    pub local_version_manager: Arc<LocalVersionManager>,
    pub obj_client: Arc<dyn ObjectStore>,
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,
}

pub struct Compactor;

impl Compactor {
    async fn run_compact(
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
                        Box::new(SSTableIterator::new(table.clone()))
                    })
                    .chain(non_overlapping_table_seqs.iter().map(
                        |tableseq| -> Box<dyn HummockIterator> {
                            Box::new(ConcatIterator::new(tableseq.clone()))
                        },
                    )),
            );

            let spawn_context = SubCompactContext {
                options: context.options.clone(),
                local_version_manager: context.local_version_manager.clone(),
                obj_client: context.obj_client.clone(),
                hummock_meta_client: context.hummock_meta_client.clone(),
            };
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
                            spawn_context,
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
        let mut sub_compact_results = Vec::with_capacity(num_sub);

        while let Some(tokio_result) = buffered.next().await {
            let (sub_result, sub_kr_idx, sub_output) = tokio_result.unwrap();
            sub_compact_outputsets.push((sub_kr_idx, sub_output));
            sub_compact_results.push(sub_result);
        }

        sub_compact_outputsets.sort_by_key(|(sub_kr_idx, _)| *sub_kr_idx);
        for (_, sub_output) in sub_compact_outputsets {
            for sstable in sub_output {
                compact_task.sorted_output_ssts.push(SstableInfo {
                    id: sstable.id,
                    key_range: Some(risingwave_pb::hummock::KeyRange {
                        left: sstable.meta.get_smallest_key().to_vec(),
                        right: sstable.meta.get_largest_key().to_vec(),
                        inf: false,
                    }),
                });
            }
        }

        for sub_compact_result in sub_compact_results {
            sub_compact_result?;
        }

        Ok(())
    }

    async fn sub_compact(
        context: SubCompactContext,
        kr: KeyRange,
        mut iter: MergeIterator<'_>,
        local_sorted_output_ssts: &mut Vec<SSTable>,
        is_target_ultimate_and_leveling: bool,
        watermark: Epoch,
    ) -> HummockResult<()> {
        // NOTICE: should be user_key overlap, NOT full_key overlap!
        let has_user_key_overlap = !is_target_ultimate_and_leveling;

        if !kr.left.is_empty() {
            iter.seek(&kr.left).await?;
        } else {
            iter.rewind().await?;
        }

        let mut skip_key = BytesMut::new();
        let mut last_key = BytesMut::new();

        let mut builder = CapacitySplitTableBuilder::new(|| async {
            let table_id = context.hummock_meta_client.get_new_table_id().await?;
            let builder = HummockStorage::get_builder(&context.options);
            Ok((table_id, builder))
        });

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

            builder
                .add_full_key(FullKey::from_slice(iter_key), iter.value(), is_new_user_key)
                .await?;

            iter.next().await?;
        }

        // Seal table for each split
        builder.seal_current();

        local_sorted_output_ssts.reserve(builder.len());
        // TODO: decide upload concurrency
        for (table_id, blocks, meta) in builder.finish() {
            let table = gen_remote_sstable(
                context.obj_client.clone(),
                table_id,
                blocks,
                meta,
                context.options.remote_dir.as_str(),
                Some(context.local_version_manager.block_cache.clone()),
            )
            .await?;
            local_sorted_output_ssts.push(table);
        }

        Ok(())
    }

    pub async fn compact(context: &SubCompactContext) -> HummockResult<()> {
        let mut compact_task = match context.hummock_meta_client.get_compaction_task().await? {
            Some(task) => task,
            None => return Ok(()),
        };

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
}

#[cfg(test)]
mod tests {
    use std::iter::once;
    use std::sync::Arc;

    use super::*;
    use crate::hummock::key::Epoch;
    use crate::hummock::local_version_manager::LocalVersionManager;
    use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
    use crate::hummock::{HummockOptions, HummockResult, HummockStorage};
    use crate::object::InMemObjectStore;

    #[tokio::test]
    // TODO #2649 compactor test should base on HummockManager in meta crate
    #[ignore]
    async fn test_same_key_not_splitted() -> HummockResult<()> {
        let options = HummockOptions::small_for_test();
        let object_client = Arc::new(InMemObjectStore::new());
        let local_version_manager = Arc::new(LocalVersionManager::new(
            object_client.clone(),
            &options.remote_dir,
            None,
        ));
        let hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        )));
        let mut storage = HummockStorage::new(
            object_client,
            options,
            local_version_manager,
            hummock_meta_client,
        )
        .await
        .unwrap();
        storage.shutdown_compactor().await.unwrap();
        let sub_compact_context = SubCompactContext {
            options: storage.options.clone(),
            local_version_manager: storage.local_version_manager.clone(),
            obj_client: storage.obj_client.clone(),
            hummock_meta_client: storage.hummock_meta_client.clone(),
        };

        let kv_count = 8192;
        let epoch: u64 = 1;
        for _ in 0..kv_count {
            storage
                .write_batch(
                    once((b"same_key".to_vec(), HummockValue::Put(b"value".to_vec()))),
                    epoch,
                )
                .await?;
        }

        let mut compact_task = storage
            .hummock_meta_client
            .get_compaction_task()
            .await?
            .unwrap();
        compact_task.watermark = Epoch::MIN; // do not gc these records
        Compactor::run_compact(&sub_compact_context, &mut compact_task).await?;

        let output_table_count = compact_task.sorted_output_ssts.len();
        assert_eq!(output_table_count, 1); // should not split into multiple tables

        let _table = compact_task.sorted_output_ssts.get(0).unwrap();
        // todo: assert that output table reaches the target size
        // assert!(table.meta.estimated_size > target_table_size); // even if it reaches the target
        // size

        Ok(())
    }
}
