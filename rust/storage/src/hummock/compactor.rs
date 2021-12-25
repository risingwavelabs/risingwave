use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::BytesMut;

use super::cloud::gen_remote_table;
use super::iterator::{ConcatIterator, HummockIterator, SortedIterator};
use super::key::get_ts;
use super::version_cmp::VersionedComparator;
use super::version_manager::{CompactTask, Level, LevelEntry};
use super::{
    HummockError, HummockResult, HummockStorage, HummockValue, Table, TableBuilder, TableIterator,
};
pub struct Compactor;
impl Compactor {
    /// Seals current table builder to generate a remote table, then returns a new table builder if
    /// `is_last_table_builder` == true
    ///
    /// # Arguments
    ///
    /// * `storage` - To get a unique ID for table to generate
    /// * `output_ssts` - Vec to which the table generated will be pushed
    /// * `table_builder` - Contains current elements
    /// * `is_last_table_builder` - if True, returns a new empty table builder
    async fn seal_table(
        storage: &Arc<HummockStorage>,
        output_ssts: &mut Vec<Table>,
        mut table_builder: TableBuilder,
        is_last_table_builder: bool,
    ) -> HummockResult<Option<TableBuilder>> {
        if !table_builder.is_empty() {
            // TODO: avoid repeating code in write_batch()
            let (blocks, meta) = table_builder.finish();
            let table_id = storage.unique_id.fetch_add(1, Ordering::SeqCst);
            let remote_dir = Some(storage.options.remote_dir.as_str());
            let table = gen_remote_table(
                storage.obj_client.clone(),
                table_id,
                blocks,
                meta,
                remote_dir,
            )
            .await?;

            output_ssts.push(table);

            if is_last_table_builder {
                return Ok(None);
            } else {
                table_builder = HummockStorage::get_builder(&storage.options);
            }
        }
        Ok(Some(table_builder))
    }

    async fn run_compact(
        storage: &Arc<HummockStorage>,
        compact_task: &mut CompactTask,
    ) -> HummockResult<()> {
        let mut iters = vec![];
        for LevelEntry { level, .. } in &compact_task.input_ssts {
            match level {
                Level::Tiering(input_sst_ids) => {
                    let tables = storage.version_manager.pick_few_tables(input_sst_ids)?;
                    iters.extend(tables.into_iter().map(|table| -> Box<dyn HummockIterator> {
                        Box::new(TableIterator::new(table))
                    }));
                }
                Level::Leveling(input_sst_ids) => {
                    let tables = storage.version_manager.pick_few_tables(input_sst_ids)?;
                    iters.push(Box::new(ConcatIterator::new(tables)));
                }
            }
        }

        let mut iter = SortedIterator::new(iters);

        compact_task
            .sorted_output_ssts
            .reserve(compact_task.splits.len());

        // TODO: we can speed up by parallelling compaction (each with different kr) (#2115)
        let mut skip_key = BytesMut::new();
        for kr in &compact_task.splits {
            // NOTICE: should be user_key overlap, NOT full_key overlap!
            let has_user_key_overlap = !compact_task.is_target_ultimate_and_leveling;

            if !kr.left.is_empty() {
                iter.seek(&kr.left).await?;
            } else {
                iter.rewind().await?;
            }

            skip_key.clear();
            let mut last_key = BytesMut::new();

            let mut table_builder = HummockStorage::get_builder(&storage.options);

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

                if last_key.is_empty() || !VersionedComparator::same_user_key(iter_key, &last_key) {
                    if !kr.right.is_empty()
                        && VersionedComparator::compare_key(iter_key, &kr.right)
                            != std::cmp::Ordering::Less
                    {
                        break;
                    }

                    if table_builder.reach_capacity() {
                        table_builder = Compactor::seal_table(
                            storage,
                            &mut compact_task.sorted_output_ssts,
                            table_builder,
                            false,
                        )
                        .await?
                        .unwrap();
                        continue;
                    }

                    last_key.clear();
                    last_key.extend_from_slice(iter_key);
                }

                let ts = get_ts(iter_key);

                if ts < compact_task.watermark {
                    skip_key = BytesMut::from(iter_key);
                    if matches!(iter.value(), HummockValue::Delete) && !has_user_key_overlap {
                        iter.next().await?;
                        continue;
                    }
                }

                table_builder.add(
                    iter_key,
                    match iter.value() {
                        HummockValue::Put(slice_val) => HummockValue::Put(Vec::from(slice_val)),
                        HummockValue::Delete => HummockValue::Delete,
                    },
                );

                iter.next().await?;
            }

            Compactor::seal_table(
                storage,
                &mut compact_task.sorted_output_ssts,
                table_builder,
                true,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn compact(storage: &Arc<HummockStorage>) -> HummockResult<()> {
        let mut compact_task = match storage.version_manager.get_compact_task().await {
            Ok(task) => task,
            Err(HummockError::OK) => {
                return Ok(());
            }
            Err(err) => {
                return Err(err);
            }
        };

        compact_task.result = Compactor::run_compact(storage, &mut compact_task).await;
        if compact_task.result.is_err() {
            for _sst_to_delete in &compact_task.sorted_output_ssts {
                // TODO: delete these tables in (S3) storage
                // However, if we request a table_id from hummock storage service every time we
                // generate a table, we would not delete here, or we should notify
                // hummock storage service to delete them.
            }
            compact_task.sorted_output_ssts.clear();
        }
        let is_task_ok = compact_task.result.is_ok();

        storage
            .version_manager
            .report_compact_task(compact_task)
            .await;

        if is_task_ok {
            Ok(())
        } else {
            Err(HummockError::ObjectIoError(String::from(
                "compaction failed.",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

    use super::*;
    use crate::hummock::iterator::BoxedHummockIterator;
    use crate::hummock::utils::bloom_filter_tables;
    use crate::hummock::version_manager::ScopedUnpinSnapshot;
    use crate::hummock::{key_with_ts, user_key, HummockOptions, HummockResult, HummockStorage};
    use crate::object::InMemObjectStore;

    #[tokio::test]
    async fn test_basic() -> HummockResult<()> {
        let hummock_storage = Arc::new(HummockStorage::new(
            Arc::new(InMemObjectStore::new()),
            HummockOptions {
                table_size: 1048576,
                block_size: 1024,
                bloom_false_positive: 0.1,
                remote_dir: String::from(""),
                checksum_algo: ChecksumAlg::Crc32c,
                stats_enabled: false,
            },
            None,
        ));

        let anchor = Bytes::from("qa");

        // First batch inserts the anchor and others.
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
            )
            .await
            .unwrap();

        // Get the value after flushing to remote.
        let value = hummock_storage
            .get_snapshot()
            .get(&anchor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Bytes::from(value), Bytes::from("111"));

        // Test looking for a nonexistent key. `next()` would return the next key.
        let value = hummock_storage
            .get_snapshot()
            .get(&Bytes::from("ab"))
            .await
            .unwrap();
        assert_eq!(value, None);

        // Write second batch.
        hummock_storage
            .write_batch(
                batch2
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .unwrap();
        Compactor::compact(&hummock_storage).await?;
        // Get the value after flushing to remote.
        let value = hummock_storage
            .get_snapshot()
            .get(&anchor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Bytes::from(value), Bytes::from("111111"));

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        let scoped_snapshot =
            ScopedUnpinSnapshot::from_version_manager(hummock_storage.version_manager.clone());
        let snapshot = scoped_snapshot.snapshot();

        for level in &snapshot.levels {
            match level {
                Level::Tiering(table_ids) => {
                    let tables = bloom_filter_tables(
                        hummock_storage.version_manager.pick_few_tables(table_ids)?,
                        &anchor,
                    )?;
                    table_iters.extend(
                        tables.into_iter().map(|table| {
                            Box::new(TableIterator::new(table)) as BoxedHummockIterator
                        }),
                    )
                }
                Level::Leveling(table_ids) => {
                    let tables = bloom_filter_tables(
                        hummock_storage.version_manager.pick_few_tables(table_ids)?,
                        &anchor,
                    )?;
                    table_iters.push(Box::new(ConcatIterator::new(tables)))
                }
            }
        }

        let mut it = SortedIterator::new(table_iters);

        it.seek(&key_with_ts(anchor.to_vec(), u64::MAX)).await?;

        assert_eq!(user_key(it.key()), anchor);
        assert_eq!(it.value().into_put_value().unwrap(), Bytes::from("111111"));

        it.next().await?;

        assert!(!it.is_valid() || user_key(it.key()) != anchor);

        // Write second batch.
        hummock_storage
            .write_batch(
                batch3
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .unwrap();
        Compactor::compact(&hummock_storage).await?;

        // Get the value after flushing to remote.
        let value = hummock_storage.get_snapshot().get(&anchor).await.unwrap();
        assert_eq!(value, None);

        // Get non-existent maximum key.
        let value = hummock_storage
            .get_snapshot()
            .get(&Bytes::from("ff"))
            .await
            .unwrap();
        assert_eq!(value, None);

        Ok(())
    }
}
