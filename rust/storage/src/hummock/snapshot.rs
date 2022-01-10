use std::ops::Bound::*;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::iterator::{
    BoxedHummockIterator, ConcatIterator, HummockIterator, SortedIterator, UserKeyIterator,
};
use super::key::{key_with_ts, user_key};
use super::utils::bloom_filter_tables;
use super::version_manager::{Level, Snapshot, VersionManager};
use super::{HummockResult, TableIterator};
use crate::hummock::iterator::{ReverseSortedIterator, ReverseUserKeyIterator};
use crate::hummock::ReverseTableIterator;

pub struct HummockSnapshot {
    /// [`epoch`] is served as timestamp and indicates when a new log appends to a SST.
    /// It is encoded into the full key.
    epoch: u64,
    /// [`version`] will increase when we edit the SST file and can be represent a specific version
    /// of storage. An edition can be adding a SST, removing a SST or compacting some SSTs.
    version: u64,
    vm: Arc<VersionManager>,
    /// TODO: remove the version once we can pin a ts.
    temp_version: Arc<Snapshot>,
}
impl Drop for HummockSnapshot {
    fn drop(&mut self) {
        self.vm.unpin(self.version)
    }
}

impl HummockSnapshot {
    pub fn new(vm: Arc<VersionManager>) -> Self {
        // TODO: Modify this line once we support ts-aware compaction and `ts` pinning.
        // Currently the compactor cannot perform ts-aware compaction so we need to pin a
        // snapshot(a set of table IDs actually) to make sure the old SSTs not be recycled by the
        // compactor. In the future we will be able to perform snapshot read relying on the latest
        // SST files and this line should be modified.
        let (version, snapshot) = vm.pin();
        let epoch = vm.max_epoch();
        Self {
            epoch,
            version,
            vm,
            temp_version: snapshot,
        }
    }

    pub async fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>> {
        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();

        // TODO: use the latest version once the ts-aware compaction is realized.
        // let scoped_snapshot = ScopedUnpinSnapshot::from_version_manager(self.vm.clone());
        // let snapshot = scoped_snapshot.snapshot();
        let snapshot = self.temp_version.clone();

        for level in &snapshot.levels {
            match level {
                Level::Tiering(table_ids) => {
                    let tables = bloom_filter_tables(self.vm.pick_few_tables(table_ids)?, key)?;
                    table_iters.extend(
                        tables.into_iter().map(|table| {
                            Box::new(TableIterator::new(table)) as BoxedHummockIterator
                        }),
                    )
                }
                Level::Leveling(table_ids) => {
                    let tables = bloom_filter_tables(self.vm.pick_few_tables(table_ids)?, key)?;
                    table_iters.push(Box::new(ConcatIterator::new(tables)))
                }
            }
        }

        let mut it = SortedIterator::new(table_iters);

        // Use `SortedIterator` to seek for they key with latest version to
        // get the latest key.
        it.seek(&key_with_ts(key.to_vec(), self.epoch)).await?;

        // Iterator has seeked passed the borders.
        if !it.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        match user_key(it.key()) == key {
            true => Ok(it.value().into_put_value().map(|x| x.to_vec())),
            false => Ok(None),
        }
    }

    pub async fn range_scan<R, B>(&self, key_range: R) -> HummockResult<UserKeyIterator>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        // TODO: use the latest version once the ts-aware compaction is realized.
        // let scoped_snapshot = ScopedUnpinSnapshot::from_version_manager(self.vm.clone());
        // let snapshot = scoped_snapshot.snapshot();
        let snapshot = self.temp_version.clone();

        // Filter out tables that overlap with given `key_range`
        let overlapped_tables = self.vm.tables(snapshot)?.into_iter().filter(|t| {
            let table_start = user_key(t.meta.smallest_key.as_slice());
            let table_end = user_key(t.meta.largest_key.as_slice());

            //        RANGE
            // TABLE
            let too_left = match key_range.start_bound() {
                Included(range_start) => range_start.as_ref() > table_end,
                Excluded(_) => unimplemented!("excluded begin key is not supported"),
                Unbounded => false,
            };
            // RANGE
            //        TABLE
            let too_right = match key_range.end_bound() {
                Included(range_end) => range_end.as_ref() < table_start,
                Excluded(range_end) => range_end.as_ref() <= table_start,
                Unbounded => false,
            };

            !too_left && !too_right
        });

        let table_iters =
            overlapped_tables.map(|t| Box::new(TableIterator::new(t)) as BoxedHummockIterator);
        let si = SortedIterator::new(table_iters);

        // TODO: avoid this clone
        Ok(UserKeyIterator::new_with_ts(
            si,
            (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            ),
            self.epoch,
        ))
    }

    /// Since `Range` always includes `start`, so if we want to scan from `end_key`(inclusive) to
    /// `begin_key`(either inclusive or exclusive), we construct a range that is [``end_key``,
    /// ``start_key``]
    pub async fn reverse_range_scan<R, B>(
        &self,
        key_range: R,
    ) -> HummockResult<ReverseUserKeyIterator>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        // TODO: use the latest version once the ts-aware compaction is realized.
        // let scoped_snapshot = ScopedUnpinSnapshot::from_version_manager(self.vm.clone());
        // let snapshot = scoped_snapshot.snapshot();
        let snapshot = self.temp_version.clone();

        // Filter out tables that overlap with given `key_range`
        let overlapped_tables = self.vm.tables(snapshot)?.into_iter().filter(|t| {
            let table_start = user_key(t.meta.smallest_key.as_slice());
            let table_end = user_key(t.meta.largest_key.as_slice());

            //        RANGE
            // TABLE
            let too_left = match key_range.end_bound() {
                Included(range_start) => range_start.as_ref() > table_end,
                Excluded(range_start) => range_start.as_ref() >= table_end,
                Unbounded => false,
            };
            // RANGE
            //        TABLE
            let too_right = match key_range.start_bound() {
                Included(range_end) => range_end.as_ref() < table_start,
                Excluded(_) => unimplemented!("excluded end key is not supported"),
                Unbounded => false,
            };

            !too_left && !too_right
        });

        let reverse_table_iters = overlapped_tables
            .map(|t| Box::new(ReverseTableIterator::new(t)) as BoxedHummockIterator);
        let reverse_sorted_iterator = ReverseSortedIterator::new(reverse_table_iters);

        // TODO: avoid this clone
        Ok(ReverseUserKeyIterator::new_with_ts(
            reverse_sorted_iterator,
            (
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
            ),
            self.epoch,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::cloud::gen_remote_table;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, iterator_test_key_of, iterator_test_key_of_ts,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::TableBuilder;
    use crate::object::{InMemObjectStore, ObjectStore};

    const TEST_KEY_TABLE_ID: u64 = 233;

    async fn gen_and_upload_table(
        obj_client: Arc<dyn ObjectStore>,
        vm: &VersionManager,
        kv_pairs: Vec<(usize, HummockValue<Vec<u8>>)>,
        epoch: u64,
    ) {
        if kv_pairs.is_empty() {
            return;
        }
        let table_id = vm.generate_table_id().await;

        let mut b = TableBuilder::new(default_builder_opt_for_test());
        for kv in kv_pairs {
            b.add(
                &iterator_test_key_of_ts(TEST_KEY_TABLE_ID, kv.0, epoch),
                kv.1,
            );
        }
        let (data, meta) = b.finish();
        // get remote table
        let table = gen_remote_table(obj_client, table_id, data, meta, None)
            .await
            .unwrap();
        vm.add_single_l0_sst(table, epoch).await.unwrap();
    }

    macro_rules! assert_count_range_scan {
        ($snapshot:expr, $range:expr, $expect_count:expr) => {{
            let mut it = $snapshot.range_scan::<_, Vec<u8>>($range).await.unwrap();
            it.rewind().await.unwrap();
            let mut count = 0;
            while it.is_valid() {
                count += 1;
                it.next().await.unwrap();
            }
            assert_eq!(count, $expect_count);
        }};
    }

    macro_rules! assert_count_reverse_range_scan {
        ($snapshot:expr, $range:expr, $expect_count:expr) => {{
            let mut it = $snapshot
                .reverse_range_scan::<_, Vec<u8>>($range)
                .await
                .unwrap();
            it.rewind().await.unwrap();
            let mut count = 0;
            while it.is_valid() {
                count += 1;
                it.next().await.unwrap();
            }
            assert_eq!(count, $expect_count);
        }};
    }

    #[tokio::test]
    async fn test_snapshot() {
        let vm = Arc::new(VersionManager::new());
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let mut epoch: u64 = 1;
        gen_and_upload_table(
            obj_client.clone(),
            &vm,
            vec![
                (1, HummockValue::Put(b"test".to_vec())),
                (2, HummockValue::Put(b"test".to_vec())),
            ],
            epoch,
        )
        .await;
        let snapshot_1 = HummockSnapshot::new(vm.clone());
        assert_count_range_scan!(snapshot_1, .., 2);

        epoch += 1;
        gen_and_upload_table(
            obj_client.clone(),
            &vm,
            vec![
                (1, HummockValue::Delete),
                (3, HummockValue::Put(b"test".to_vec())),
                (4, HummockValue::Put(b"test".to_vec())),
            ],
            epoch,
        )
        .await;
        let snapshot_2 = HummockSnapshot::new(vm.clone());
        assert_count_range_scan!(snapshot_2, .., 3);
        assert_count_range_scan!(snapshot_1, .., 2);

        epoch += 1;
        gen_and_upload_table(
            obj_client.clone(),
            &vm,
            vec![
                (2, HummockValue::Delete),
                (3, HummockValue::Delete),
                (4, HummockValue::Delete),
            ],
            epoch,
        )
        .await;
        let snapshot_3 = HummockSnapshot::new(vm.clone());
        assert_count_range_scan!(snapshot_3, .., 0);
        assert_count_range_scan!(snapshot_2, .., 3);
        assert_count_range_scan!(snapshot_1, .., 2);
    }

    #[tokio::test]
    async fn test_snapshot_range_scan() {
        let vm = Arc::new(VersionManager::new());
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;

        gen_and_upload_table(
            obj_client.clone(),
            &vm,
            vec![
                (1, HummockValue::Put(b"test".to_vec())),
                (2, HummockValue::Put(b"test".to_vec())),
                (3, HummockValue::Put(b"test".to_vec())),
                (4, HummockValue::Put(b"test".to_vec())),
            ],
            1,
        )
        .await;

        macro_rules! key {
            ($idx:expr) => {
                user_key(&iterator_test_key_of(TEST_KEY_TABLE_ID, $idx)).to_vec()
            };
        }

        let snapshot = HummockSnapshot::new(vm.clone());
        assert_count_range_scan!(snapshot, key!(2)..=key!(3), 2);
        assert_count_range_scan!(snapshot, key!(2)..key!(3), 1);
        assert_count_range_scan!(snapshot, key!(2).., 3);
        assert_count_range_scan!(snapshot, ..=key!(3), 3);
        assert_count_range_scan!(snapshot, ..key!(3), 2);
        assert_count_range_scan!(snapshot, .., 4);
    }

    #[tokio::test]
    async fn test_snapshot_reverse_range_scan() {
        let vm = Arc::new(VersionManager::new());
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;

        gen_and_upload_table(
            obj_client.clone(),
            &vm,
            vec![
                (1, HummockValue::Put(b"test".to_vec())),
                (2, HummockValue::Put(b"test".to_vec())),
                (3, HummockValue::Put(b"test".to_vec())),
                (4, HummockValue::Put(b"test".to_vec())),
            ],
            1,
        )
        .await;

        macro_rules! key {
            ($idx:expr) => {
                user_key(&iterator_test_key_of(TEST_KEY_TABLE_ID, $idx)).to_vec()
            };
        }

        let snapshot = HummockSnapshot::new(vm.clone());
        assert_count_reverse_range_scan!(snapshot, key!(3)..=key!(2), 2);
        assert_count_reverse_range_scan!(snapshot, key!(3)..key!(2), 1);
        assert_count_reverse_range_scan!(snapshot, key!(3)..key!(1), 2);
        assert_count_reverse_range_scan!(snapshot, key!(3)..=key!(1), 3);
        assert_count_reverse_range_scan!(snapshot, key!(3)..key!(0), 3);
        assert_count_reverse_range_scan!(snapshot, .., 4);
    }
}
