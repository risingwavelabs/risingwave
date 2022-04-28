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
use std::ops::RangeBounds;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::key::{key_with_epoch, user_key, FullKey};
use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::LevelType;

use super::iterator::{
    BoxedForwardHummockIterator, ConcatIterator, DirectedUserIterator, MergeIterator,
    ReverseConcatIterator, ReverseMergeIterator, ReverseUserIterator, UserIterator,
};
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::utils::{validate_epoch, validate_table_key_range};
use super::{HummockStorage, ReverseSSTableIterator, SSTableIterator};
use crate::error::StorageResult;
use crate::hummock::iterator::BoxedBackwardHummockIterator;
use crate::hummock::utils::prune_ssts;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

impl HummockStorage {
    async fn iter_inner<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        reversed: bool,
    ) -> StorageResult<HummockStateStoreIter>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        // if `reverse` is true, use `overlapped_backward_sstable_iters`, otherwise use
        // `overlapped_forward_sstable_iters`
        let mut overlapped_forward_iters = vec![];
        let mut overlapped_backward_iters = vec![];

        let (uncommitted_ssts, pinned_version) = {
            let read_version = self.local_version_manager.read_version(epoch)?;

            // Check epoch validity
            validate_epoch(read_version.pinned_version.safe_epoch(), epoch)?;
            let levels = read_version.pinned_version.levels();
            validate_table_key_range(levels)?;

            // Generate shared buffer iterators
            for shared_buffer in read_version.shared_buffer {
                for batch in shared_buffer.get_overlap_batches(&key_range, reversed) {
                    if reversed {
                        overlapped_backward_iters
                            .push(Box::new(batch.into_backward_iter())
                                as BoxedBackwardHummockIterator)
                    } else {
                        overlapped_forward_iters
                        .push(Box::new(batch.into_forward_iter()) as BoxedForwardHummockIterator)
                    }
                }
            }

            (read_version.uncommitted_ssts, read_version.pinned_version)
        };

        // Generate iterators for uncommitted ssts by filter out ssts that do not overlap with given
        // `key_range`
        let table_infos = prune_ssts(uncommitted_ssts.iter(), &key_range, reversed);
        for table_info in table_infos.into_iter().rev() {
            let table = self.sstable_store.sstable(table_info.id).await?;
            if reversed {
                overlapped_backward_iters.push(Box::new(ReverseSSTableIterator::new(
                    table,
                    self.sstable_store(),
                )) as BoxedBackwardHummockIterator);
            } else {
                overlapped_forward_iters
                    .push(Box::new(SSTableIterator::new(table, self.sstable_store()))
                        as BoxedForwardHummockIterator);
            };
        }

        // Generate iterators for versioned ssts by filter out ssts that do not overlap with given
        // `key_range`
        for level in pinned_version.levels() {
            let table_infos = prune_ssts(level.get_table_infos().iter(), &key_range, reversed);
            if table_infos.is_empty() {
                continue;
            }

            match level.level_type() {
                LevelType::Overlapping => {
                    for table_info in table_infos.into_iter().rev() {
                        let table = self.sstable_store.sstable(table_info.id).await?;
                        if reversed {
                            overlapped_backward_iters.push(Box::new(ReverseSSTableIterator::new(
                                table,
                                self.sstable_store(),
                            ))
                                as BoxedBackwardHummockIterator);
                        } else {
                            overlapped_forward_iters.push(Box::new(SSTableIterator::new(
                                table,
                                self.sstable_store(),
                            ))
                                as BoxedForwardHummockIterator);
                        };
                    }
                }
                LevelType::Nonoverlapping => {
                    if reversed {
                        overlapped_backward_iters.push(Box::new(ReverseConcatIterator::new(
                            table_infos.into_iter().rev().cloned().collect(),
                            self.sstable_store(),
                        ))
                            as BoxedBackwardHummockIterator);
                    } else {
                        overlapped_forward_iters.push(Box::new(ConcatIterator::new(
                            table_infos.into_iter().cloned().collect(),
                            self.sstable_store(),
                        ))
                            as BoxedForwardHummockIterator);
                    };
                }
            }
        }

        assert!(
            (reversed && overlapped_forward_iters.is_empty())
                || (!reversed && overlapped_backward_iters.is_empty())
        );

        self.stats
            .iter_merge_sstable_counts
            .observe((overlapped_forward_iters.len() + overlapped_backward_iters.len()) as f64);

        let mut user_iterator = if reversed {
            let reverse_merge_iterator =
                ReverseMergeIterator::new(overlapped_backward_iters, self.stats.clone());
            DirectedUserIterator::Backward(ReverseUserIterator::with_epoch(
                reverse_merge_iterator,
                (
                    key_range.end_bound().map(|b| b.as_ref().to_owned()),
                    key_range.start_bound().map(|b| b.as_ref().to_owned()),
                ),
                epoch,
                Some(pinned_version),
            ))
        } else {
            let merge_iterator = MergeIterator::new(overlapped_forward_iters, self.stats.clone());

            DirectedUserIterator::Forward(UserIterator::new(
                merge_iterator,
                (
                    key_range.start_bound().map(|b| b.as_ref().to_owned()),
                    key_range.end_bound().map(|b| b.as_ref().to_owned()),
                ),
                epoch,
                Some(pinned_version),
            ))
        };

        user_iterator.rewind().await?;
        Ok(HummockStateStoreIter::new(user_iterator))
    }
}

impl StateStore for HummockStorage {
    type Iter<'a> = HummockStateStoreIter;

    define_state_store_associated_type!();

    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'_> {
        async move {
            let (uncommitted_ssts, pinned_version) = {
                let read_version = self.local_version_manager.read_version(epoch)?;

                // check epoch validity
                validate_epoch(read_version.pinned_version.safe_epoch(), epoch)?;

                // Query shared buffer. Return the value without iterating SSTs if found
                for shared_buffer in read_version.shared_buffer {
                    for batch in shared_buffer.get_overlap_batches(&(key..=key), false) {
                        match batch.get(key) {
                            Some(v) => {
                                self.stats.get_shared_buffer_hit_counts.inc();
                                return Ok(v.into_user_value().map(|v| v.into()));
                            }
                            None => continue,
                        }
                    }
                }

                (read_version.uncommitted_ssts, read_version.pinned_version)
            };

            let mut table_counts = 0;
            let internal_key = key_with_epoch(key.to_vec(), epoch);

            // Query uploaded but uncommitted SSTs. Return the value if found.
            let table_infos = prune_ssts(uncommitted_ssts.iter(), &(key..=key), false);
            for table_info in table_infos.into_iter().rev() {
                let table = self.sstable_store.sstable(table_info.id).await?;
                table_counts += 1;
                if let Some(v) = self.get_from_table(table, &internal_key, key).await? {
                    return Ok(Some(v));
                }
            }

            for level in pinned_version.levels() {
                if level.table_infos.is_empty() {
                    continue;
                }
                match level.level_type() {
                    LevelType::Overlapping => {
                        let table_infos = prune_ssts(level.table_infos.iter(), &(key..=key), false);
                        for table_info in table_infos.into_iter().rev() {
                            let table = self.sstable_store.sstable(table_info.id).await?;
                            table_counts += 1;
                            if let Some(v) = self.get_from_table(table, &internal_key, key).await? {
                                return Ok(Some(v));
                            }
                        }
                    }
                    LevelType::Nonoverlapping => {
                        let table_idx = level
                            .table_infos
                            .partition_point(|table| {
                                let ord = VersionedComparator::compare_key(
                                    user_key(&table.key_range.as_ref().unwrap().left),
                                    key,
                                );
                                ord == Ordering::Less || ord == Ordering::Equal
                            })
                            .saturating_sub(1); // considering the boundary of 0
                        assert!(table_idx < level.table_infos.len());
                        table_counts += 1;
                        // Because we will keep multiple version of one in the same sst file, we
                        // do not find it in the next adjacent file.
                        let table = self
                            .sstable_store
                            .sstable(level.table_infos[table_idx].id)
                            .await?;
                        if let Some(v) = self.get_from_table(table, &internal_key, key).await? {
                            return Ok(Some(v));
                        }
                    }
                }
            }

            self.stats
                .iter_merge_sstable_counts
                .observe(table_counts as f64);
            Ok(None)
        }
    }

    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { self.iter(key_range, epoch).await?.collect(limit).await }
    }

    fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            self.reverse_iter(key_range, epoch)
                .await?
                .collect(limit)
                .await
        }
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
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let batch = SharedBufferBatch::new(
                kv_pairs
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                            value.into(),
                        )
                    })
                    .collect_vec(),
                epoch,
            );
            let size = batch.size();

            self.local_version_manager
                .write_shared_buffer(epoch, batch, false)
                .await?;

            if !self.options.async_checkpoint_enabled {
                self.local_version_manager()
                    .sync_shared_buffer(Some(epoch))
                    .await?;
            }
            Ok(size)
        }
    }

    /// Replicates a batch to shared buffer, without uploading to the storage backend.
    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move {
            let batch = SharedBufferBatch::new(
                kv_pairs
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                            value.into(),
                        )
                    })
                    .collect_vec(),
                epoch,
            );

            self.local_version_manager
                .write_shared_buffer(epoch, batch, true)
                .await?;

            Ok(())
        }
    }

    /// Returns an iterator that scan from the begin key to the end key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn iter<R, B>(&self, key_range: R, epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        self.iter_inner(key_range, epoch, false)
    }

    /// Returns a reversed iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        self.iter_inner(key_range, epoch, true)
    }

    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { Ok(self.local_version_manager.wait_epoch(epoch).await?) }
    }

    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            self.local_version_manager()
                .sync_shared_buffer(epoch)
                .await?;
            Ok(())
        }
    }
}

pub struct HummockStateStoreIter {
    inner: DirectedUserIterator,
}

impl HummockStateStoreIter {
    fn new(inner: DirectedUserIterator) -> Self {
        Self { inner }
    }

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
