#![allow(dead_code)]

use std::ops::Bound::*;
use std::ops::RangeBounds;
use std::sync::Arc;

use risingwave_pb::hummock::LevelType;

use super::iterator::{
    BoxedHummockIterator, ConcatIterator, HummockIterator, MergeIterator, UserIterator,
};
use super::key::{key_with_epoch, user_key};
use super::utils::bloom_filter_sstables;
use super::{HummockResult, SSTableIterator};
use crate::hummock::iterator::{ReverseMergeIterator, ReverseUserIterator};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::{HummockEpoch, ReverseSSTableIterator};

pub struct HummockSnapshot {
    /// [`epoch`] is served as epoch and indicates when a new log appends to a SST.
    /// It is encoded into the full key.
    epoch: HummockEpoch,
    vm: Arc<LocalVersionManager>,
}
impl Drop for HummockSnapshot {
    fn drop(&mut self) {
        // TODO unpin the epoch
    }
}

impl HummockSnapshot {
    pub fn new(epoch: HummockEpoch, vm: Arc<LocalVersionManager>) -> Self {
        Self { epoch, vm }
    }

    pub async fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>> {
        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();

        let version = self.vm.get_scoped_local_version();

        for level in &version.merged_version() {
            match level.level_type() {
                LevelType::Overlapping => {
                    let tables = bloom_filter_sstables(
                        self.vm.pick_few_tables(&level.table_ids).await?,
                        key,
                    )?;
                    table_iters.extend(
                        tables.into_iter().map(|table| {
                            Box::new(SSTableIterator::new(table)) as BoxedHummockIterator
                        }),
                    )
                }
                LevelType::Nonoverlapping => {
                    let tables = bloom_filter_sstables(
                        self.vm.pick_few_tables(&level.table_ids).await?,
                        key,
                    )?;
                    table_iters.push(Box::new(ConcatIterator::new(tables)))
                }
            }
        }

        let mut it = MergeIterator::new(table_iters);

        // Use `MergeIterator` to seek for they key with latest version to
        // get the latest key.
        it.seek(&key_with_epoch(key.to_vec(), self.epoch)).await?;

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

    pub async fn range_scan<R, B>(&'_ self, key_range: R) -> HummockResult<UserIterator<'_>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let version = self.vm.get_scoped_local_version();

        // Filter out tables that overlap with given `key_range`
        let overlapped_tables = self
            .vm
            .tables(&version.merged_version())
            .await?
            .into_iter()
            .filter(|t| {
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
            overlapped_tables.map(|t| Box::new(SSTableIterator::new(t)) as BoxedHummockIterator);
        let mi = MergeIterator::new(table_iters);

        // TODO: avoid this clone
        Ok(UserIterator::new_with_epoch(
            mi,
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
        &'_ self,
        key_range: R,
    ) -> HummockResult<ReverseUserIterator<'_>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let version = self.vm.get_scoped_local_version();

        // Filter out tables that overlap with given `key_range`
        let overlapped_tables = self
            .vm
            .tables(&version.merged_version())
            .await?
            .into_iter()
            .filter(|t| {
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
            .map(|t| Box::new(ReverseSSTableIterator::new(t)) as BoxedHummockIterator);
        let reverse_merge_iterator = ReverseMergeIterator::new(reverse_table_iters);

        // TODO: avoid this clone
        Ok(ReverseUserIterator::new_with_epoch(
            reverse_merge_iterator,
            (
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
            ),
            self.epoch,
        ))
    }
}
