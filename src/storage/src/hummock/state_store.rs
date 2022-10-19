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

use std::ops::Bound::{Excluded, Included};
use std::ops::RangeBounds;

use bytes::Bytes;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::key::next_key;
use risingwave_hummock_sdk::HummockReadEpoch;
use tracing::log::warn;

use super::iterator::{BackwardUserIterator, UserIterator};
use super::{
    BackwardSstableIterator, HummockStorage, HummockStorageIterator, SstableIterator,
    SstableIteratorType,
};
use crate::error::StorageResult;
use crate::hummock::iterator::{
    Backward, BackwardUserIteratorType, DirectedUserIteratorBuilder, DirectionEnum, Forward,
    ForwardUserIteratorType, HummockIteratorDirection,
};
use crate::hummock::store::{ReadOptions as ReadOptionsV2, StateStore as StateStoreV2};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore};

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

    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move { Ok(self.local_version_manager.try_wait_epoch(epoch).await?) }
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
