// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::ops::Bound;
use std::sync::atomic::Ordering as MemOrdering;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::key::{map_table_key_range, TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::hummock::SstableInfo;
use tokio::sync::oneshot;
use tracing::log::warn;

use super::store::state_store::HummockStorageIterator;
use super::store::version::CommittedVersion;
use super::utils::validate_safe_epoch;
use super::HummockStorage;
use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::store::memtable::ImmutableMemtable;
use crate::hummock::store::state_store::LocalHummockStorage;
use crate::hummock::store::version::read_filter_for_batch;
use crate::hummock::{HummockEpoch, HummockError};
use crate::monitor::StoreLocalStatistic;
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
        let key_range = (
            Bound::Included(TableKey(key.to_vec())),
            Bound::Included(TableKey(key.to_vec())),
        );

        let read_version_tuple = if read_options.read_version_from_backup {
            self.build_read_version_tuple_from_backup(epoch).await?
        } else {
            self.build_read_version_tuple(epoch, read_options.table_id, &key_range)?
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
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIterator>> {
        let read_version_tuple = if read_options.read_version_from_backup {
            self.build_read_version_tuple_from_backup(epoch).await?
        } else {
            self.build_read_version_tuple(epoch, read_options.table_id, &key_range)?
        };

        self.hummock_version_reader
            .iter(key_range, epoch, read_options, read_version_tuple)
            .await
    }

    async fn build_read_version_tuple_from_backup(
        &self,
        epoch: u64,
    ) -> StorageResult<(Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion)> {
        match self.backup_reader.try_get_hummock_version(epoch).await {
            Ok(Some(backup_version)) => {
                validate_safe_epoch(backup_version.safe_epoch(), epoch)?;
                Ok((Vec::default(), Vec::default(), backup_version))
            }
            Ok(None) => Err(HummockError::read_backup_error(format!(
                "backup include epoch {} not found",
                epoch
            ))
            .into()),
            Err(e) => Err(e),
        }
    }

    fn build_read_version_tuple(
        &self,
        epoch: u64,
        table_id: TableId,
        key_range: &TableKeyRange,
    ) -> StorageResult<(Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion)> {
        let pinned_version = self.pinned_version.load();
        validate_safe_epoch(pinned_version.safe_epoch(), epoch)?;

        // check epoch if lower mce
        let read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion) =
            if epoch <= pinned_version.max_committed_epoch() {
                // read committed_version directly without build snapshot
                (Vec::default(), Vec::default(), (**pinned_version).clone())
            } else {
                let read_version_vec = {
                    let read_guard = self.read_version_mapping.read();
                    read_guard
                        .get(&table_id)
                        .map(|v| v.values().cloned().collect_vec())
                        .unwrap_or(Vec::new())
                };

                // When the system has just started and no state has been created, the memory state
                // may be empty
                if read_version_vec.is_empty() {
                    (Vec::default(), Vec::default(), (**pinned_version).clone())
                } else {
                    read_filter_for_batch(epoch, table_id, key_range, read_version_vec)?
                }
            };

        Ok(read_version_tuple)
    }
}

impl StateStoreRead for HummockStorage {
    type IterStream = StreamTypeOfIter<HummockStorageIterator>;

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
            self.validate_read_epoch(wait_epoch.clone())?;
            let wait_epoch = match wait_epoch {
                HummockReadEpoch::Committed(epoch) => {
                    assert_ne!(epoch, HummockEpoch::MAX, "epoch should not be u64::MAX");
                    epoch
                }
                _ => return Ok(()),
            };
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
                        return Err(HummockError::wait_epoch("tx dropped").into());
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
                .send(HummockEvent::AwaitSyncEpoch {
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
        // Update `seal_epoch` synchronously,
        // as `HummockEvent::SealEpoch` is handled asynchronously.
        assert!(epoch > self.seal_epoch.load(MemOrdering::SeqCst));
        self.seal_epoch.store(epoch, MemOrdering::SeqCst);
        if is_checkpoint {
            let _ = self.min_current_epoch.compare_exchange(
                HummockEpoch::MAX,
                epoch,
                MemOrdering::SeqCst,
                MemOrdering::SeqCst,
            );
        }
        self.hummock_event_sender
            .send(HummockEvent::SealEpoch {
                epoch,
                is_checkpoint,
            })
            .expect("should send success");
        StoreLocalStatistic::flush_all();
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        self.min_current_epoch
            .store(HummockEpoch::MAX, MemOrdering::SeqCst);
        async move {
            let (tx, rx) = oneshot::channel();
            self.hummock_event_sender
                .send(HummockEvent::Clear(tx))
                .expect("should send success");
            rx.await.expect("should wait success");
            Ok(())
        }
    }

    fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_> {
        async move { self.new_local_inner(table_id).await }
    }

    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()> {
        if let HummockReadEpoch::Current(read_current_epoch) = epoch {
            assert_ne!(
                read_current_epoch,
                HummockEpoch::MAX,
                "epoch should not be u64::MAX"
            );
            let sealed_epoch = self.seal_epoch.load(MemOrdering::SeqCst);
            if read_current_epoch > sealed_epoch {
                return Err(HummockError::read_current_epoch(format!(
                    "Cannot read when cluster is under recovery. read {} > max seal epoch {}",
                    read_current_epoch, sealed_epoch
                ))
                .into());
            }

            let min_current_epoch = self.min_current_epoch.load(MemOrdering::SeqCst);
            if read_current_epoch < min_current_epoch {
                return Err(HummockError::read_current_epoch(format!(
                    "Cannot read when cluster is under recovery. read {} < min current epoch {}",
                    read_current_epoch, min_current_epoch
                ))
                .into());
            }
        }
        Ok(())
    }
}

impl HummockStorage {
    #[cfg(any(test, feature = "test"))]
    pub async fn seal_and_sync_epoch(&self, epoch: u64) -> StorageResult<SyncResult> {
        self.seal_epoch(epoch, true);
        self.sync(epoch).await
    }
}
