use std::sync::Arc;

use bytes::BytesMut;
use futures::stream::{self, StreamExt};

use super::iterator::{ConcatIterator, HummockIterator, MergeIterator};
use super::key::{get_epoch, Epoch, FullKey};
use super::key_range::KeyRange;
use super::multi_builder::CapacitySplitTableBuilder;
use super::version_cmp::VersionedComparator;
use super::version_manager::{CompactMetrics, CompactTask, Level, LevelEntry, TableSetStatistics};
use super::{
    HummockError, HummockOptions, HummockResult, HummockStorage, HummockValue, SSTable,
    SSTableIterator, VersionManager,
};
use crate::hummock::cloud::gen_remote_sstable;
use crate::object::ObjectStore;

pub struct SubCompactContext {
    pub options: Arc<HummockOptions>,
    pub version_manager: Arc<VersionManager>,
    pub obj_client: Arc<dyn ObjectStore>,
}

pub struct Compactor;

impl Compactor {
    async fn run_compact(
        context: &SubCompactContext,
        compact_task: &mut CompactTask,
    ) -> HummockResult<()> {
        let mut overlapping_tables = vec![];
        let mut non_overlapping_table_seqs = vec![];
        let target_level = compact_task.target_level;
        let accumulating_readsize =
            |metrics: &mut CompactMetrics, level_idx: u8, tables: &Vec<Arc<SSTable>>| {
                let read_statistics: &mut TableSetStatistics = if level_idx == target_level {
                    &mut metrics.read_level_nplus1
                } else {
                    &mut metrics.read_level_n
                };
                for table in tables {
                    read_statistics.add_table(table);
                }
            };
        for LevelEntry { level_idx, level } in &compact_task.input_ssts {
            match level {
                Level::Tiering(input_sst_ids) => {
                    let tables = context.version_manager.pick_few_tables(input_sst_ids)?;
                    accumulating_readsize(&mut compact_task.metrics, *level_idx, &tables);
                    overlapping_tables.extend(tables);
                }
                Level::Leveling(input_sst_ids) => {
                    let tables = context.version_manager.pick_few_tables(input_sst_ids)?;
                    accumulating_readsize(&mut compact_task.metrics, *level_idx, &tables);
                    non_overlapping_table_seqs.push(tables);
                }
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
                version_manager: context.version_manager.clone(),
                obj_client: context.obj_client.clone(),
            };
            let spawn_kr = kr.clone();
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
        for (_, mut sub_output) in sub_compact_outputsets {
            for table in &sub_output {
                compact_task.metrics.write.add_table(table);
            }
            compact_task.sorted_output_ssts.append(&mut sub_output);
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
            let table_id = context.version_manager.generate_table_id().await;
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
                .add_full_key(
                    FullKey::from_slice(iter_key),
                    iter.value().to_owned_value(),
                    is_new_user_key,
                )
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
                // Will panic in production mode
                None,
            )
            .await?;
            local_sorted_output_ssts.push(table);
        }

        Ok(())
    }

    pub async fn compact(context: &SubCompactContext) -> HummockResult<()> {
        let mut compact_task = match context.version_manager.get_compact_task().await? {
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
        context
            .version_manager
            .report_compact_task(compact_task, result)
            .await;

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

    use bytes::Bytes;
    use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

    use super::*;
    use crate::hummock::iterator::BoxedHummockIterator;
    use crate::hummock::key::{key_with_epoch, Epoch};
    use crate::hummock::local_version_manager::LocalVersionManager;
    use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
    use crate::hummock::utils::bloom_filter_sstables;
    use crate::hummock::version_manager::{ScopedUnpinSnapshot, VersionManager};
    use crate::hummock::{user_key, HummockOptions, HummockResult, HummockStorage};
    use crate::object::InMemObjectStore;

    #[tokio::test]
    // TODO #2649 compactor test should base on HummockManager in meta crate
    #[ignore]
    async fn test_basic() -> HummockResult<()> {
        let object_client = Arc::new(InMemObjectStore::new());
        let remote_dir = "";
        let hummock_storage = Arc::new(
            HummockStorage::new(
                object_client.clone(),
                HummockOptions {
                    sstable_size: 1048576,
                    block_size: 1024,
                    bloom_false_positive: 0.1,
                    remote_dir: remote_dir.to_string(),
                    checksum_algo: ChecksumAlg::Crc32c,
                },
                Arc::new(VersionManager::new()),
                Arc::new(LocalVersionManager::new(object_client, remote_dir, None)),
                Arc::new(MockHummockMetaClient::new(Arc::new(
                    MockHummockMetaService::new(),
                ))),
            )
            .await
            .unwrap(),
        );
        let sub_compact_context = SubCompactContext {
            options: hummock_storage.options.clone(),
            version_manager: hummock_storage.version_manager.clone(),
            obj_client: hummock_storage.obj_client.clone(),
        };

        let anchor = Bytes::from("qa");

        // First batch inserts the anchor and others.
        let mut epoch: u64 = 0;
        let mut batch1 = vec![
            (anchor.clone(), Some(Bytes::from("111"))),
            (Bytes::from("bb"), Some(Bytes::from("222"))),
        ];

        // Make sure the batch is sorted.
        batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Second batch modifies the anchor.
        let mut batch2 = vec![
            (Bytes::from("cc"), Some(Bytes::from("333"))),
            (anchor.clone(), Some(Bytes::from("111111"))),
        ];

        // Make sure the batch is sorted.
        batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Third batch deletes the anchor
        let mut batch3 = vec![
            (Bytes::from("dd"), Some(Bytes::from("444"))),
            (Bytes::from("ee"), Some(Bytes::from("555"))),
            (anchor.clone(), None),
        ];

        // Make sure the batch is sorted.
        batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        // Write first batch.
        hummock_storage
            .write_batch(
                batch1
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
                epoch,
            )
            .await
            .unwrap();

        // Get the value after flushing to remote.
        let value = hummock_storage.get(&anchor, epoch).await.unwrap().unwrap();
        assert_eq!(Bytes::from(value), Bytes::from("111"));

        // Test looking for a nonexistent key. `next()` would return the next key.
        let value = hummock_storage
            .get(&Bytes::from("ab"), epoch)
            .await
            .unwrap();
        assert_eq!(value, None);

        // Write second batch.
        epoch += 1;
        hummock_storage
            .write_batch(
                batch2
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
                epoch,
            )
            .await
            .unwrap();
        Compactor::compact(&sub_compact_context).await?;
        // Get the value after flushing to remote.
        let value = hummock_storage.get(&anchor, epoch).await.unwrap().unwrap();
        assert_eq!(Bytes::from(value), Bytes::from("111111"));

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        let scoped_snapshot =
            ScopedUnpinSnapshot::from_version_manager(hummock_storage.version_manager.clone());
        let snapshot = scoped_snapshot.snapshot();

        for level in &snapshot.levels {
            match level {
                Level::Tiering(table_ids) => {
                    let tables = bloom_filter_sstables(
                        hummock_storage.version_manager.pick_few_tables(table_ids)?,
                        &anchor,
                    )?;
                    table_iters.extend(
                        tables.into_iter().map(|table| {
                            Box::new(SSTableIterator::new(table)) as BoxedHummockIterator
                        }),
                    )
                }
                Level::Leveling(table_ids) => {
                    let tables = bloom_filter_sstables(
                        hummock_storage.version_manager.pick_few_tables(table_ids)?,
                        &anchor,
                    )?;
                    table_iters.push(Box::new(ConcatIterator::new(tables)))
                }
            }
        }

        let mut it = MergeIterator::new(table_iters);

        it.seek(&key_with_epoch(anchor.to_vec(), u64::MAX)).await?;

        assert_eq!(user_key(it.key()), anchor);
        assert_eq!(it.value().into_put_value().unwrap(), Bytes::from("111111"));

        it.next().await?;

        assert!(!it.is_valid() || user_key(it.key()) != anchor);

        // Write third batch.
        epoch += 1;
        hummock_storage
            .write_batch(
                batch3
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
                epoch,
            )
            .await
            .unwrap();
        Compactor::compact(&sub_compact_context).await?;

        // Get the value after flushing to remote.
        let value = hummock_storage.get(&anchor, epoch).await.unwrap();
        assert_eq!(value, None);

        // Get non-existent maximum key.
        let value = hummock_storage
            .get(&Bytes::from("ff"), epoch)
            .await
            .unwrap();
        assert_eq!(value, None);

        Ok(())
    }

    #[tokio::test]
    // TODO #2649 compactor test should base on HummockManager in meta crate
    #[ignore]
    async fn test_same_key_not_splitted() -> HummockResult<()> {
        let options = HummockOptions::small_for_test();
        let object_client = Arc::new(InMemObjectStore::new());
        let version_manager = Arc::new(VersionManager::new());
        let local_version_manager = Arc::new(LocalVersionManager::new(
            object_client.clone(),
            &options.remote_dir,
            None,
        ));
        let target_table_size = options.sstable_size;
        let hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        )));
        let mut storage = HummockStorage::new(
            object_client,
            options,
            version_manager,
            local_version_manager,
            hummock_meta_client,
        )
        .await
        .unwrap();
        storage.shutdown_compactor().await.unwrap();
        let sub_compact_context = SubCompactContext {
            options: storage.options.clone(),
            version_manager: storage.version_manager.clone(),
            obj_client: storage.obj_client.clone(),
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

        let mut compact_task = storage.version_manager.get_compact_task().await?.unwrap();
        compact_task.watermark = Epoch::MIN; // do not gc these records
        Compactor::run_compact(&sub_compact_context, &mut compact_task).await?;

        let output_table_count = compact_task.sorted_output_ssts.len();
        assert_eq!(output_table_count, 1); // should not split into multiple tables

        let table = compact_task.sorted_output_ssts.get(0).unwrap();
        assert!(table.meta.estimated_size > target_table_size); // even if it reaches the target size

        Ok(())
    }
}
