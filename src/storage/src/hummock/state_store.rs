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

use std::future::Future;
use std::ops::Bound::{Excluded, Included};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_pb::hummock::VNodeBitmap;

use super::iterator::{
    BackwardConcatIterator, BackwardMergeIterator, BackwardUserIterator,
    BoxedForwardHummockIterator, ConcatIterator, DirectedUserIterator, MergeIterator, UserIterator,
};
use super::utils::{can_concat, search_sst_idx, validate_epoch, validate_table_key_range};
use super::{BackwardSSTableIterator, HummockStorage, SSTableIterator, SSTableIteratorType};
use crate::error::StorageResult;
use crate::hummock::iterator::{BoxedBackwardHummockIterator, ReadOptions};
use crate::hummock::utils::prune_ssts;
use crate::monitor::StoreLocalStatistic;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

impl HummockStorage {
    async fn iter_inner<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        backward: bool,
    ) -> StorageResult<HummockStateStoreIter>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let read_options = Arc::new(ReadOptions::default());
        // if `backward` is true, use `overlapped_backward_sstable_iters`, otherwise use
        // `overlapped_forward_sstable_iters`
        let mut overlapped_forward_iters = vec![];
        let mut overlapped_backward_iters = vec![];

        let (uncommitted_ssts, pinned_version) = {
            let read_version = self.local_version_manager.read_version(epoch);

            // Check epoch validity
            validate_epoch(read_version.pinned_version.safe_epoch(), epoch)?;
            let levels = read_version.pinned_version.levels();
            validate_table_key_range(levels)?;

            // Generate shared buffer iterators
            for shared_buffer in read_version.shared_buffer {
                for batch in shared_buffer.get_overlap_batches(&key_range, backward) {
                    if backward {
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
        let mut stats = StoreLocalStatistic::default();
        let table_infos = prune_ssts(uncommitted_ssts.iter(), &key_range, backward, None);
        for table_info in table_infos.into_iter().rev() {
            let table = self
                .sstable_store
                .sstable(table_info.id, &mut stats)
                .await?;
            if backward {
                overlapped_backward_iters.push(Box::new(BackwardSSTableIterator::create(
                    table,
                    self.sstable_store(),
                    read_options.clone(),
                )) as BoxedBackwardHummockIterator);
            } else {
                overlapped_forward_iters.push(Box::new(SSTableIterator::create(
                    table,
                    self.sstable_store(),
                    read_options.clone(),
                )) as BoxedForwardHummockIterator);
            };
        }

        // Generate iterators for versioned ssts by filter out ssts that do not overlap with given
        // `key_range`
        for level in pinned_version.levels() {
            let table_infos =
                prune_ssts(level.get_table_infos().iter(), &key_range, backward, None);
            if table_infos.is_empty() {
                continue;
            }
            if can_concat(&table_infos) {
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

                if backward {
                    overlapped_backward_iters.push(Box::new(BackwardConcatIterator::new(
                        matched_table_infos
                            .iter()
                            .rev()
                            .map(|&info| info.clone())
                            .collect(),
                        self.sstable_store(),
                        read_options.clone(),
                    ))
                        as BoxedBackwardHummockIterator);
                } else {
                    overlapped_forward_iters.push(Box::new(ConcatIterator::new(
                        matched_table_infos
                            .iter()
                            .map(|&info| info.clone())
                            .collect_vec(),
                        self.sstable_store(),
                        read_options.clone(),
                    ))
                        as BoxedForwardHummockIterator);
                };
            } else {
                for table_info in table_infos.into_iter().rev() {
                    let table = self
                        .sstable_store
                        .sstable(table_info.id, &mut stats)
                        .await?;
                    if backward {
                        overlapped_backward_iters.push(Box::new(BackwardSSTableIterator::new(
                            table,
                            self.sstable_store(),
                        ))
                            as BoxedBackwardHummockIterator);
                    } else {
                        overlapped_forward_iters.push(Box::new(SSTableIterator::new(
                            table,
                            self.sstable_store(),
                            read_options.clone(),
                        ))
                            as BoxedForwardHummockIterator);
                    };
                }
            }
        }

        assert!(
            (backward && overlapped_forward_iters.is_empty())
                || (!backward && overlapped_backward_iters.is_empty())
        );

        self.stats
            .iter_merge_sstable_counts
            .observe((overlapped_forward_iters.len() + overlapped_backward_iters.len()) as f64);

        let mut user_iterator = if backward {
            let backward_merge_iterator =
                BackwardMergeIterator::new(overlapped_backward_iters, self.stats.clone());
            DirectedUserIterator::Backward(BackwardUserIterator::with_epoch(
                backward_merge_iterator,
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

    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get_with_vnode_set<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        vnode_set: Option<VNodeBitmap>,
    ) -> StorageResult<Option<Bytes>> {
        let mut stats = StoreLocalStatistic::default();
        let (uncommitted_ssts, pinned_version) = {
            let read_version = self.local_version_manager.read_version(epoch);

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
        let table_infos = prune_ssts(
            uncommitted_ssts.iter(),
            &(key..=key),
            false,
            vnode_set.as_ref(),
        );
        let read_options = Arc::new(ReadOptions::default());
        for table_info in table_infos.into_iter().rev() {
            let table = self
                .sstable_store
                .sstable(table_info.id, &mut stats)
                .await?;
            table_counts += 1;
            if let Some(v) = self
                .get_from_table(table, &internal_key, key, read_options.clone(), &mut stats)
                .await?
            {
                return Ok(Some(v));
            }
        }

        for level in pinned_version.levels() {
            if level.table_infos.is_empty() {
                continue;
            }
             {
                    let table_infos = prune_ssts(
                        level.table_infos.iter(),
                        &(key..=key),
                        false,
                        vnode_set.as_ref(),
                    );
                    for table_info in table_infos.into_iter().rev() {
                        let table = self.sstable_store.sstable(table_info.id, &mut stats).await?;
                        table_counts += 1;
                        if let Some(v) = self.get_from_table(table, &internal_key, key, read_options.clone(), &mut stats).await? {
                            return Ok(Some(v));
                        }
                    }
                }
                /*
                LevelType::Nonoverlapping => {
                    let table_idx = search_sst_idx(level, key);
                    assert!(table_idx < level.table_infos.len());
                    if vnode_set.is_none()
                        || bitmap_overlap(
                            vnode_set.as_ref().unwrap(),
                            level.table_infos[table_idx].get_vnode_bitmaps(),
                        )
                    {
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
                */
        }

        stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .observe(table_counts as f64);
        Ok(None)
    }
}

impl StateStore for HummockStorage {
    type Iter = HummockStateStoreIter;

    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'_> {
        async move { self.get_with_vnode_set(key, epoch, None).await }
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

    fn backward_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::BackwardScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            self.backward_iter(key_range, epoch)
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
            let size = self
                .local_version_manager
                .write_shared_buffer(epoch, kv_pairs, false)
                .await?;
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
            self.local_version_manager
                .write_shared_buffer(epoch, kv_pairs, true)
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

    /// Returns a backward iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn backward_iter<R, B>(&self, key_range: R, epoch: u64) -> Self::BackwardIterFuture<'_, R, B>
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
