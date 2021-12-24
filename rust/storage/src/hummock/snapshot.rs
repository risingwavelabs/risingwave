use std::sync::Arc;

use super::iterator::{
    BoxedHummockIterator, ConcatIterator, HummockIterator, SortedIterator, UserKeyIterator,
};
use super::key::{key_with_ts, user_key, FullKey};
use super::utils::bloom_filter_tables;
use super::version_manager::{Level, Snapshot, VersionManager};
use super::{HummockResult, TableIterator};

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
        begin_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> HummockResult<UserKeyIterator> {
        if begin_key.is_some() && end_key.is_some() {
            assert!(begin_key.clone().unwrap() <= end_key.clone().unwrap());
        }

        let begin_key_copy = match &begin_key {
            Some(begin_key) => key_with_ts(begin_key.clone(), u64::MAX),
            None => Vec::new(),
        };
        let begin_fk = FullKey::from_slice(begin_key_copy.as_slice());

        let end_key_copy = match &end_key {
            Some(end_key) => key_with_ts(end_key.clone(), self.ts),
            None => Vec::new(),
        };
        let end_fk = FullKey::from_slice(end_key_copy.as_slice());

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();
        // TODO: use the latest version once the ts-aware compaction is realized.
        // let scoped_snapshot = ScopedUnpinSnapshot::from_version_manager(self.vm.clone());
        // let snapshot = scoped_snapshot.snapshot();
        let snapshot = self.temp_version.clone();
        for table in self.vm.tables(snapshot)? {
            let tlk = FullKey::from_slice(table.meta.largest_key.as_slice());
            let table_too_left = begin_key.is_some() && tlk < begin_fk;

            let tsk = FullKey::from_slice(table.meta.smallest_key.as_slice());
            let table_too_right = end_key.is_some() && tsk > end_fk;

            // decide whether the two ranges have common sub-range
            if !(table_too_left || table_too_right) {
                let iter = Box::new(TableIterator::new(table.clone()));
                table_iters.push(iter);
            }
        }

        let si = SortedIterator::new(table_iters);
        Ok(UserKeyIterator::new_with_ts(
            si, begin_key, end_key, self.ts,
        ))
    }
}
