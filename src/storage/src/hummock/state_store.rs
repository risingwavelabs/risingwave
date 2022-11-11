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
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::Ordering as MemOrdering;
use std::time::Duration;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::key::{map_table_key_range, next_key, TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockReadEpoch;
use tokio::sync::oneshot;
use tracing::log::warn;

use super::store::state_store::HummockStorageIterator;
use super::utils::validate_epoch;
use super::HummockStorage;
use crate::error::{StorageError, StorageResult};
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::store::state_store::LocalHummockStorage;
use crate::hummock::store::version::read_filter_for_batch;
use crate::hummock::{HummockEpoch, HummockError};
use crate::store::*;
use crate::{
    define_state_store_associated_type, define_state_store_read_associated_type, StateStore,
};

impl HummockStorage {
    /// Gets the value of a specified `key` in the table specified in `read_options`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(
        &self,
        key: &[u8],
        epoch: HummockEpoch,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let pinned_version = self.pinned_version.load();
        let table_id = read_options.table_id;
        validate_epoch(pinned_version.safe_epoch(), epoch)?;

        // check epoch if lower mce
        let read_version_tuple = if epoch <= pinned_version.max_committed_epoch() {
            // read committed_version directly without build snapshot
            (Vec::default(), Vec::default(), (**pinned_version).clone())
        } else {
            // TODO: use read_version_mapping for batch query
            let read_version_vec = vec![self.read_version.clone()];
            let key_range = (
                Bound::Included(TableKey(key.to_vec())),
                Bound::Included(TableKey(key.to_vec())),
            );
            read_filter_for_batch(epoch, table_id, &key_range, read_version_vec)?
        };

        self.hummock_version_reader
            .get(TableKey(key), epoch, read_options, read_version_tuple)
            .await
    }

    async fn iter_inner(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let pinned_version = self.pinned_version.load();
        let table_id = read_options.table_id;
        validate_epoch(pinned_version.safe_epoch(), epoch)?;

        // check epoch if lower mce
        let read_version_tuple = if epoch <= pinned_version.max_committed_epoch() {
            // read committed_version directly without build snapshot
            (Vec::default(), Vec::default(), (**pinned_version).clone())
        } else {
            // TODO: use read_version_mapping for batch query
            let read_version_vec = vec![self.read_version.clone()];
            read_filter_for_batch(epoch, table_id, &key_range, read_version_vec)?
        };

        self.hummock_version_reader
            .iter(key_range, epoch, read_options, read_version_tuple)
            .await
    }
}

impl StateStoreRead for HummockStorage {
    type Iter = HummockStorageIterator;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        self.get(key, epoch, read_options)
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        if let Some(prefix_hint) = read_options.prefix_hint.as_ref() {
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

        self.iter_inner(map_table_key_range(key_range), epoch, read_options)
    }
}

impl StateStore for HummockStorage {
    type Local = LocalHummockStorage;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

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

    fn new_local(&self, _table_id: TableId) -> Self::NewLocalFuture<'_> {
        async move {
            LocalHummockStorage::new(
                self.read_version.clone(),
                self.hummock_version_reader.clone(),
                self.hummock_event_sender.clone(),
                self.buffer_tracker.get_memory_limiter().clone(),
                #[cfg(not(madsim))]
                self.tracing.clone(),
            )
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
