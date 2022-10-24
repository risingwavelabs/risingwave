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

use std::cmp::Ordering;
use std::future::Future;
use std::ops::Bound::{Excluded, Included};
use std::ops::RangeBounds;
use std::sync::atomic::Ordering as MemOrdering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use minitrace::future::FutureExt;
use minitrace::Span;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::key::{key_with_epoch, next_key, user_key};
use risingwave_hummock_sdk::{can_concat, HummockReadEpoch};
use risingwave_pb::hummock::LevelType;
use tracing::log::warn;

use super::iterator::{
    BackwardUserIterator, ConcatIteratorInner, DirectedUserIterator, HummockIteratorUnion,
    UserIterator,
};
use super::utils::{search_sst_idx, validate_epoch};
use super::{
    get_from_order_sorted_uncommitted_data, get_from_sstable_info, hit_sstable_bloom_filter,
    BackwardSstableIterator, HummockStorage, HummockStorageIterator, SstableIterator,
    SstableIteratorType,
};
use crate::error::{StorageError, StorageResult};
use crate::hummock::iterator::{
    Backward, BackwardUserIteratorType, DirectedUserIteratorBuilder, DirectionEnum, Forward,
    ForwardUserIteratorType, HummockIteratorDirection,
};
use crate::hummock::local_version::ReadVersion;
use crate::hummock::shared_buffer::build_ordered_merge_iter;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::store::{ReadOptions as ReadOptionsV2, StateStore as StateStoreV2};
use crate::hummock::utils::prune_ssts;
use crate::hummock::{HummockEpoch, HummockError, HummockResult};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

pub(crate) trait HummockIteratorType {
    type Direction: HummockIteratorDirection;
    type SstableIteratorType: SstableIteratorType<Direction = Self::Direction>;
    type UserIteratorBuilder: DirectedUserIteratorBuilder<
        Direction = Self::Direction,
        SstableIteratorType = Self::SstableIteratorType,
    >;

    fn direction() -> DirectionEnum {
        Self::Direction::direction()
    }
}

pub(crate) struct ForwardIter;
pub(crate) struct BackwardIter;

impl HummockIteratorType for ForwardIter {
    type Direction = Forward;
    type SstableIteratorType = SstableIterator;
    type UserIteratorBuilder = UserIterator<ForwardUserIteratorType>;
}

impl HummockIteratorType for BackwardIter {
    type Direction = Backward;
    type SstableIteratorType = BackwardSstableIterator;
    type UserIteratorBuilder = BackwardUserIterator<BackwardUserIteratorType>;
}

impl HummockStorage {
    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get<'a>(
        &'a self,
        key: &'a [u8],
        check_bloom_filter: bool,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let read_options_v2 = ReadOptionsV2 {
            prefix_hint: None,
            check_bloom_filter,
            table_id: read_options.table_id,
            retention_seconds: read_options.retention_seconds,
        };

        self.storage_core
            .get(key, read_options.epoch, read_options_v2)
            .await
    }

    #[allow(dead_code)]
    async fn old_get<'a>(
        &'a self,
        key: &'a [u8],
        check_bloom_filter: bool,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let epoch = read_options.epoch;
        let table_id = read_options.table_id;
        let mut local_stats = StoreLocalStatistic::default();
        let ReadVersion {
            shared_buffer_data,
            pinned_version,
            sync_uncommitted_data,
        } = self.read_filter(&read_options, &(key..=key))?;

        let mut table_counts = 0;
        let internal_key = key_with_epoch(key.to_vec(), epoch);

        // Query shared buffer. Return the value without iterating SSTs if found
        for uncommitted_data in shared_buffer_data {
            // iterate over uncommitted data in order index in descending order
            let (value, table_count) = get_from_order_sorted_uncommitted_data(
                self.sstable_store.clone(),
                uncommitted_data,
                &internal_key,
                &mut local_stats,
                key,
                check_bloom_filter,
            )
            .await?;
            if let Some(v) = value {
                local_stats.report(self.stats.as_ref());
                return Ok(v.into_user_value());
            }
            table_counts += table_count;
        }
        for sync_uncommitted_data in sync_uncommitted_data {
            let (value, table_count) = get_from_order_sorted_uncommitted_data(
                self.sstable_store.clone(),
                sync_uncommitted_data,
                &internal_key,
                &mut local_stats,
                key,
                check_bloom_filter,
            )
            .await?;
            if let Some(v) = value {
                local_stats.report(self.stats.as_ref());
                return Ok(v.into_user_value());
            }
            table_counts += table_count;
        }

        // See comments in HummockStorage::iter_inner for details about using compaction_group_id in
        // read/write path.
        assert!(pinned_version.is_valid());
        for level in pinned_version.levels(table_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos =
                        prune_ssts(level.table_infos.iter(), table_id, &(key..=key));
                    for sstable_info in sstable_infos {
                        table_counts += 1;
                        if let Some(v) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            &internal_key,
                            check_bloom_filter,
                            &mut local_stats,
                        )
                        .await?
                        {
                            local_stats.report(self.stats.as_ref());
                            return Ok(v.into_user_value());
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx = level.table_infos.partition_point(|table| {
                        let ord =
                            user_key(&table.key_range.as_ref().unwrap().left).cmp(key.as_ref());
                        ord == Ordering::Less || ord == Ordering::Equal
                    });
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = user_key(
                        &level.table_infos[table_info_idx]
                            .key_range
                            .as_ref()
                            .unwrap()
                            .right,
                    )
                    .cmp(key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        continue;
                    }

                    table_counts += 1;
                    if let Some(v) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        &internal_key,
                        check_bloom_filter,
                        &mut local_stats,
                    )
                    .await?
                    {
                        local_stats.report(self.stats.as_ref());
                        return Ok(v.into_user_value());
                    }
                }
            }
        }

        local_stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(table_counts as f64);
        Ok(None)
    }

    fn read_filter<R, B>(
        &self,
        read_options: &ReadOptions,
        key_range: &R,
    ) -> HummockResult<ReadVersion>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let epoch = read_options.epoch;
        let read_version =
            self.local_version_manager
                .read_filter(epoch, read_options.table_id, key_range);

        // Check epoch validity
        validate_epoch(read_version.pinned_version.safe_epoch(), epoch)?;

        Ok(read_version)
    }

    #[allow(dead_code)]
    async fn old_iter_inner<R, B, T>(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: R,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStateStoreIter>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
        T: HummockIteratorType,
    {
        let epoch = read_options.epoch;
        let table_id = read_options.table_id;
        let min_epoch = read_options.min_epoch();
        let iter_read_options = Arc::new(SstableIteratorReadOptions::default());
        let mut overlapped_iters = vec![];

        let ReadVersion {
            shared_buffer_data,
            pinned_version,
            sync_uncommitted_data,
        } = self.read_filter(&read_options, &key_range)?;

        let mut local_stats = StoreLocalStatistic::default();
        for uncommitted_data in shared_buffer_data {
            overlapped_iters.push(HummockIteratorUnion::Second(
                build_ordered_merge_iter::<T>(
                    &uncommitted_data,
                    self.sstable_store.clone(),
                    self.stats.clone(),
                    &mut local_stats,
                    iter_read_options.clone(),
                )
                .in_span(Span::enter_with_local_parent(
                    "build_ordered_merge_iter_shared_buffer",
                ))
                .await?,
            ));
        }
        for sync_uncommitted_data in sync_uncommitted_data {
            overlapped_iters.push(HummockIteratorUnion::Second(
                build_ordered_merge_iter::<T>(
                    &sync_uncommitted_data,
                    self.sstable_store.clone(),
                    self.stats.clone(),
                    &mut local_stats,
                    iter_read_options.clone(),
                )
                .in_span(Span::enter_with_local_parent(
                    "build_ordered_merge_iter_uncommitted",
                ))
                .await?,
            ))
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["memory-iter"])
            .observe(overlapped_iters.len() as f64);

        // Generate iterators for versioned ssts by filter out ssts that do not overlap with given
        // `key_range`

        // The correctness of using compaction_group_id in read path and write path holds only
        // because we are currently:
        //
        // a) adopting static compaction group. It means a table_id->compaction_group mapping would
        // never change since creation until the table is dropped.
        //
        // b) enforcing shared buffer to split output SSTs by compaction group. It means no SSTs
        // would contain tables from different compaction_group, even for those in L0.
        //
        // When adopting dynamic compaction group in the future, be sure to revisit this assumption.
        assert!(pinned_version.is_valid());
        for level in pinned_version.levels(table_id) {
            let table_infos = prune_ssts(level.table_infos.iter(), table_id, &key_range);
            if table_infos.is_empty() {
                continue;
            }
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&table_infos));
                let start_table_idx = match key_range.start_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => 0,
                };
                let end_table_idx = match key_range.end_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => table_infos.len().saturating_sub(1),
                };
                assert!(start_table_idx < table_infos.len() && end_table_idx < table_infos.len());
                let matched_table_infos = &table_infos[start_table_idx..=end_table_idx];

                let pruned_sstables = match T::Direction::direction() {
                    DirectionEnum::Backward => matched_table_infos.iter().rev().collect_vec(),
                    DirectionEnum::Forward => matched_table_infos.iter().collect_vec(),
                };

                let mut sstables = vec![];
                for sstable_info in pruned_sstables {
                    if let Some(bloom_filter_key) = prefix_hint.as_ref() {
                        let sstable = self
                            .sstable_store
                            .sstable(sstable_info, &mut local_stats)
                            .in_span(Span::enter_with_local_parent("get_sstable"))
                            .await?;

                        if hit_sstable_bloom_filter(
                            sstable.value(),
                            bloom_filter_key,
                            &mut local_stats,
                        ) {
                            sstables.push((*sstable_info).clone());
                        }
                    } else {
                        sstables.push((*sstable_info).clone());
                    }
                }

                overlapped_iters.push(HummockIteratorUnion::Third(ConcatIteratorInner::<
                    T::SstableIteratorType,
                >::new(
                    sstables,
                    self.sstable_store(),
                    iter_read_options.clone(),
                )));
            } else {
                for table_info in table_infos.into_iter().rev() {
                    let sstable = self
                        .sstable_store
                        .sstable(table_info, &mut local_stats)
                        .in_span(Span::enter_with_local_parent("get_sstable"))
                        .await?;
                    if let Some(bloom_filter_key) = prefix_hint.as_ref() {
                        if !hit_sstable_bloom_filter(
                            sstable.value(),
                            bloom_filter_key,
                            &mut local_stats,
                        ) {
                            continue;
                        }
                    }

                    overlapped_iters.push(HummockIteratorUnion::Fourth(
                        T::SstableIteratorType::create(
                            sstable,
                            self.sstable_store(),
                            iter_read_options.clone(),
                        ),
                    ));
                }
            }
        }

        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(overlapped_iters.len() as f64);

        let key_range = (
            key_range.start_bound().map(|b| b.as_ref().to_owned()),
            key_range.end_bound().map(|b| b.as_ref().to_owned()),
        );

        // The input of the user iterator is a `HummockIteratorUnion` of 4 different types. We use
        // the union because the underlying merge iterator
        let mut user_iterator = T::UserIteratorBuilder::create(
            overlapped_iters,
            key_range,
            epoch,
            min_epoch,
            Some(pinned_version),
        );

        user_iterator
            .rewind()
            .in_span(Span::enter_with_local_parent("rewind"))
            .await?;
        local_stats.report(self.stats.as_ref());
        Ok(HummockStateStoreIter::new(
            user_iterator,
            self.stats.clone(),
        ))
    }
}

impl StateStore for HummockStorage {
    type Iter = HummockStorageIterator;

    define_state_store_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        check_bloom_filter: bool,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move { self.get(key, check_bloom_filter, read_options).await }
    }

    fn scan<R, B>(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: R,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            self.iter(prefix_hint, key_range, read_options)
                .await?
                .collect(limit)
                .await
        }
    }

    fn backward_scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _read_options: ReadOptions,
    ) -> Self::BackwardScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    /// Writes a batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    /// * Globally unique. The streaming operators should ensure that different operators won't
    ///   operate on the same key. The operator operating on one keyspace should always wait for all
    ///   changes to be committed before reading and writing new keys to the engine. That is because
    ///   that the table with lower epoch might be committed after a table with higher epoch has
    ///   been committed. If such case happens, the outcome is non-predictable.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        self.storage_core.ingest_batch(kv_pairs, write_options)
    }

    /// Returns an iterator that scan from the begin key to the end key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn iter<R, B>(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: R,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        if let Some(prefix_hint) = prefix_hint.as_ref() {
            let next_key = next_key(prefix_hint);

            // learn more detail about start_bound with storage_table.rs.
            match key_range.start_bound() {
                // it guarantees that the start bound must be included (some different case)
                // 1. Include(pk + col_bound) => prefix_hint <= start_bound <
                // next_key(prefix_hint)
                //
                // for case2, frontend need to reject this, avoid excluded start_bound and
                // transform it to included(next_key), without this case we can just guarantee
                // that start_bound < next_key
                //
                // 2. Include(next_key(pk +
                // col_bound)) => prefix_hint <= start_bound <= next_key(prefix_hint)
                //
                // 3. Include(pk) => prefix_hint <= start_bound < next_key(prefix_hint)
                Included(range_start) | Excluded(range_start) => {
                    assert!(range_start.as_ref() >= prefix_hint.as_slice());
                    assert!(range_start.as_ref() < next_key.as_slice() || next_key.is_empty());
                }

                _ => unreachable!(),
            }

            match key_range.end_bound() {
                Included(range_end) => {
                    assert!(range_end.as_ref() >= prefix_hint.as_slice());
                    assert!(range_end.as_ref() < next_key.as_slice() || next_key.is_empty());
                }

                // 1. Excluded(end_bound_of_prefix(pk + col)) => prefix_hint < end_bound <=
                // next_key(prefix_hint)
                //
                // 2. Excluded(pk + bound) => prefix_hint < end_bound <=
                // next_key(prefix_hint)
                Excluded(range_end) => {
                    assert!(range_end.as_ref() > prefix_hint.as_slice());
                    assert!(range_end.as_ref() <= next_key.as_slice() || next_key.is_empty());
                }

                std::ops::Bound::Unbounded => {
                    assert!(next_key.is_empty());
                }
            }
        } else {
            // not check
        }

        let read_options_v2 = ReadOptionsV2 {
            prefix_hint,
            check_bloom_filter: true,
            table_id: read_options.table_id,
            retention_seconds: read_options.retention_seconds,
        };

        return self.storage_core.iter(
            (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            ),
            read_options.epoch,
            read_options_v2,
        );
    }

    /// Returns a backward iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn backward_iter<R, B>(
        &self,
        _key_range: R,
        _read_options: ReadOptions,
    ) -> Self::BackwardIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            unimplemented!();
        }
    }

    /// Waits until the local hummock version contains the epoch. If `wait_epoch` is `Current`,
    /// we will only check whether it is le `sealed_epoch` and won't wait.
    fn try_wait_epoch(&self, wait_epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move {
            // Ok(self.local_version_manager.try_wait_epoch(epoch).await?)
            let wait_epoch = match wait_epoch {
                HummockReadEpoch::Committed(epoch) => epoch,
                HummockReadEpoch::Current(epoch) => {
                    // let sealed_epoch = self.local_version.read().get_sealed_epoch();
                    let sealed_epoch = (*self.seal_epoch).load(MemOrdering::SeqCst);
                    assert!(
                        epoch <= sealed_epoch
                            && epoch != HummockEpoch::MAX
                        ,
                        "current epoch can't read, because the epoch in storage is not updated, epoch{}, sealed epoch{}"
                        ,epoch
                        ,sealed_epoch
                    );
                    return Ok(());
                }
                HummockReadEpoch::NoWait(_) => return Ok(()),
            };
            if wait_epoch == HummockEpoch::MAX {
                panic!("epoch should not be u64::MAX");
            }

            let mut receiver = self.version_update_notifier_tx.subscribe();
            let max_committed_epoch = *receiver.borrow();
            if max_committed_epoch >= wait_epoch {
                return Ok(());
            }
            loop {
                match tokio::time::timeout(Duration::from_secs(30), receiver.changed()).await {
                    Err(elapsed) => {
                        // The reason that we need to retry here is batch scan in
                        // chain/rearrange_chain is waiting for an
                        // uncommitted epoch carried by the CreateMV barrier, which
                        // can take unbounded time to become committed and propagate
                        // to the CN. We should consider removing the retry as well as wait_epoch
                        // for chain/rearrange_chain if we enforce
                        // chain/rearrange_chain to be scheduled on the same
                        // CN with the same distribution as the upstream MV.
                        // See #3845 for more details.
                        tracing::warn!(
                            "wait_epoch {:?} timeout when waiting for version update elapsed {:?}s",
                            wait_epoch,
                            elapsed
                        );
                        continue;
                    }
                    Ok(Err(_)) => {
                        return StorageResult::Err(StorageError::Hummock(
                            HummockError::wait_epoch("tx dropped"),
                        ));
                    }
                    Ok(Ok(_)) => {
                        let max_committed_epoch = *receiver.borrow();
                        if max_committed_epoch >= wait_epoch {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            if epoch == INVALID_EPOCH {
                warn!("syncing invalid epoch");
                return Ok(SyncResult {
                    sync_size: 0,
                    uncommitted_ssts: vec![],
                });
            }
            let sync_result = self
                .local_version_manager
                .await_sync_shared_buffer(epoch)
                .await?;
            Ok(sync_result)
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        if epoch == INVALID_EPOCH {
            warn!("sealing invalid epoch");
            return;
        }
        self.local_version_manager.seal_epoch(epoch, is_checkpoint);
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move {
            self.local_version_manager.clear_shared_buffer().await;
            Ok(())
        }
    }
}

impl HummockStorage {
    #[cfg(any(test, feature = "test"))]
    pub async fn seal_and_sync_epoch(&self, epoch: u64) -> StorageResult<SyncResult> {
        self.seal_epoch(epoch, true);
        self.sync(epoch).await
    }
}

pub struct HummockStateStoreIter {
    inner: DirectedUserIterator,
    metrics: Arc<StateStoreMetrics>,
}

impl HummockStateStoreIter {
    #[allow(dead_code)]
    fn new(inner: DirectedUserIterator, metrics: Arc<StateStoreMetrics>) -> Self {
        Self { inner, metrics }
    }

    #[allow(dead_code)]
    async fn collect(mut self, limit: Option<usize>) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

        for _ in 0..limit.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(kv) => kvs.push(kv),
                None => break,
            }
        }

        Ok(kvs)
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats);
    }
}

impl StateStoreIter for HummockStateStoreIter {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let iter = &mut self.inner;

            if iter.is_valid() {
                let kv = (
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                );
                iter.next().await?;
                Ok(Some(kv))
            } else {
                Ok(None)
            }
        }
    }
}

impl Drop for HummockStateStoreIter {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic(&mut stats);
        stats.report(&self.metrics);
    }
}
