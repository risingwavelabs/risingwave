use std::cmp;
use std::ops::Bound::*;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::iterator::{
    BoxedHummockIterator, ConcatIterator, HummockIterator, SortedIterator, UserKeyIterator,
};
use super::key::{key_with_ts, user_key};
use super::utils::bloom_filter_tables;
use super::version_cmp::VersionedComparator;
use super::version_manager::{Level, Snapshot, VersionManager};
use super::{HummockResult, Table, TableIterator};

pub struct HummockSnapshot {
    /// [`ts`] stands for timestamp and indicates when a new log appends to a SST.
    /// It is encoded into the full key. Currently we use table id as [`ts`].
    ts: u64,
    /// [`epoch`] will increase when we edit the SST file and can be represent a specific version
    /// of storage. An edition can be adding a SST, removing a SST or compacting some SSTs.
    epoch: u64,
    vm: Arc<VersionManager>,
    /// TODO: remove the version once we can pin a ts.
    temp_version: Arc<Snapshot>,
}
impl Drop for HummockSnapshot {
    fn drop(&mut self) {
        self.vm.unpin(self.epoch)
    }
}

impl HummockSnapshot {
    pub fn new(vm: Arc<VersionManager>) -> Self {
        // TODO: Modify this line once we support ts-aware compaction and `ts` pinning.
        // Currently the compactor cannot perform ts-aware compaction so we need to pin a
        // snapshot(a set of table IDs actually) to make sure the old SSTs not be recycled by the
        // compactor. In the future we will be able to perform snapshot read relying on the latest
        // SST files and this line should be modified.
        let (epoch, snapshot) = vm.pin();
        let ts = vm.latest_ts();
        Self {
            ts,
            epoch,
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
        it.seek(&key_with_ts(key.to_vec(), self.ts)).await?;

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

    pub async fn range_scan(
        &self,
        key_range: impl RangeBounds<Vec<u8>>,
    ) -> HummockResult<UserKeyIterator> {
        let mut table_not_too_left: Vec<Arc<Table>> = Vec::new();

        // TODO: use the latest version once the ts-aware compaction is realized.
        // let scoped_snapshot = ScopedUnpinSnapshot::from_version_manager(self.vm.clone());
        // let snapshot = scoped_snapshot.snapshot();
        let snapshot = self.temp_version.clone();

        let begin_fk = match key_range.start_bound() {
            Unbounded => {
                for table in self.vm.tables(snapshot)? {
                    table_not_too_left.push(table.clone());
                }
                Unbounded
            }
            Included(begin_uk) => {
                let begin_fk = key_with_ts(begin_uk.clone(), u64::MAX);
                for table in self.vm.tables(snapshot)? {
                    if VersionedComparator::compare_key(
                        &table.meta.largest_key,
                        begin_fk.as_slice(),
                    ) != cmp::Ordering::Less
                    {
                        table_not_too_left.push(table.clone());
                    }
                }
                Included(begin_fk)
            }
            Excluded(_) => {
                todo!()
            }
        };

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        let end_fk = match key_range.end_bound() {
            Unbounded => {
                for table in table_not_too_left {
                    table_iters.push(Box::new(TableIterator::new(table.clone())));
                }
                Unbounded
            }
            Included(end_uk) => {
                let end_fk = key_with_ts(end_uk.clone(), u64::MIN);
                for table in table_not_too_left {
                    if VersionedComparator::compare_key(
                        table.meta.smallest_key.as_slice(),
                        end_fk.as_slice(),
                    ) != cmp::Ordering::Greater
                    {
                        table_iters.push(Box::new(TableIterator::new(table.clone())));
                    }
                }
                Included(end_fk)
            }
            Excluded(_) => {
                todo!()
            }
        };

        let si = SortedIterator::new(table_iters);
        Ok(UserKeyIterator::new_with_ts(
            si,
            (begin_fk, end_fk),
            self.ts,
        ))
    }
}
