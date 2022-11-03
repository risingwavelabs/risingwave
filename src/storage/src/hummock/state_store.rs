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
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering as MemOrdering;
use std::time::Duration;

use bytes::Bytes;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::key::next_key;
use risingwave_hummock_sdk::HummockReadEpoch;
use tokio::sync::oneshot;
use tracing::log::warn;

use super::store::HummockStorageIterator;
use super::utils::validate_epoch;
use super::HummockStorage;
use crate::error::{StorageError, StorageResult};
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::store::version::read_filter_for_batch;
use crate::hummock::store::{ReadOptions as ReadOptionsV2, StateStore as StateStoreV2};
use crate::hummock::{HummockEpoch, HummockError};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore};

impl HummockStorage {
    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(
        &self,
        key: &[u8],
        check_bloom_filter: bool,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let read_options_v2 = ReadOptionsV2 {
            prefix_hint: None,
            check_bloom_filter,
            table_id: read_options.table_id,
            retention_seconds: read_options.retention_seconds,
        };

        let pinned_version = self.pinned_version.load();
        let epoch = read_options.epoch;
        let table_id = read_options.table_id;
        validate_epoch(pinned_version.safe_epoch(), epoch)?;

        // check epoch if lower mce
        let read_version_tuple = if epoch <= pinned_version.max_committed_epoch() {
            // read committed_version directly without build snapshot
            (Vec::default(), Vec::default(), (**pinned_version).clone())
        } else {
            // TODO: use read_version_mapping for batch query
            let read_version_vec = vec![self.storage_core.read_version()];
            let key_range = (Bound::Included(key.to_vec()), Bound::Included(key.to_vec()));
            read_filter_for_batch(epoch, table_id, &key_range, read_version_vec)?
        };

        self.hummock_version_reader
            .get(key, epoch, read_options_v2, read_version_tuple)
            .await
    }

    async fn iter_inner(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let read_options_v2 = ReadOptionsV2 {
            prefix_hint,
            check_bloom_filter: true,
            table_id: read_options.table_id,
            retention_seconds: read_options.retention_seconds,
        };

        let pinned_version = self.pinned_version.load();
        let epoch = read_options.epoch;
        let table_id = read_options.table_id;
        validate_epoch(pinned_version.safe_epoch(), epoch)?;

        // check epoch if lower mce
        let read_version_tuple = if epoch <= pinned_version.max_committed_epoch() {
            // read committed_version directly without build snapshot
            (Vec::default(), Vec::default(), (**pinned_version).clone())
        } else {
            // TODO: use read_version_mapping for batch query
            let read_version_vec = vec![self.storage_core.read_version()];
            read_filter_for_batch(epoch, table_id, &key_range, read_version_vec)?
        };

        self.hummock_version_reader
            .iter(key_range, epoch, read_options_v2, read_version_tuple)
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
        self.get(key, check_bloom_filter, read_options)
    }

    fn scan(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_> {
        async move {
            self.iter(prefix_hint, key_range, read_options)
                .await?
                .collect(limit)
                .await
        }
    }

    fn backward_scan(
        &self,
        _key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        _limit: Option<usize>,
        _read_options: ReadOptions,
    ) -> Self::BackwardScanFuture<'_> {
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
    fn iter(
        &self,
        prefix_hint: Option<Vec<u8>>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
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
                    assert!(range_start.as_slice() >= prefix_hint.as_slice());
                    assert!(range_start.as_slice() < next_key.as_slice() || next_key.is_empty());
                }

                _ => unreachable!(),
            }

            match key_range.end_bound() {
                Included(range_end) => {
                    assert!(range_end.as_slice() >= prefix_hint.as_slice());
                    assert!(range_end.as_slice() < next_key.as_slice() || next_key.is_empty());
                }

                // 1. Excluded(end_bound_of_prefix(pk + col)) => prefix_hint < end_bound <=
                // next_key(prefix_hint)
                //
                // 2. Excluded(pk + bound) => prefix_hint < end_bound <=
                // next_key(prefix_hint)
                Excluded(range_end) => {
                    assert!(range_end.as_slice() > prefix_hint.as_slice());
                    assert!(range_end.as_slice() <= next_key.as_slice() || next_key.is_empty());
                }

                std::ops::Bound::Unbounded => {
                    assert!(next_key.is_empty());
                }
            }
        } else {
            // not check
        }

        self.iter_inner(prefix_hint, key_range, read_options)
    }

    /// Returns a backward iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn backward_iter(
        &self,
        _key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        _read_options: ReadOptions,
    ) -> Self::BackwardIterFuture<'_> {
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
            // avoid unnecessary check in the loop if the value does not change
            let max_committed_epoch = *receiver.borrow_and_update();
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
            let (tx, rx) = oneshot::channel();
            self.hummock_event_sender
                .send(HummockEvent::SyncEpoch {
                    new_sync_epoch: epoch,
                    sync_result_sender: tx,
                })
                .expect("should send success");
            Ok(rx.await.expect("should wait success")?)
        }
    }

    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool) {
        if epoch == INVALID_EPOCH {
            warn!("sealing invalid epoch");
            return;
        }
        self.hummock_event_sender
            .send(HummockEvent::SealEpoch {
                epoch,
                is_checkpoint,
            })
            .expect("should send success");
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move {
            let (tx, rx) = oneshot::channel();
            self.hummock_event_sender
                .send(HummockEvent::Clear(tx))
                .expect("should send success");
            rx.await.expect("should wait success");
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
