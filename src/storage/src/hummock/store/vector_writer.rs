// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::HummockEpoch;

use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::event_handler::hummock_event_handler::HummockEventSender;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::utils::wait_for_epoch;
use crate::hummock::vector::writer::VectorWriterImpl;
use crate::hummock::{HummockError, HummockResult, ObjectIdManagerRef, SstableStoreRef};
use crate::opts::StorageOpts;
use crate::store::*;

struct VectorWriterInitGuard {
    table_id: TableId,
    hummock_event_sender: HummockEventSender,
}

impl VectorWriterInitGuard {
    fn new(
        table_id: TableId,
        init_epoch: HummockEpoch,
        hummock_event_sender: HummockEventSender,
    ) -> HummockResult<Self> {
        hummock_event_sender
            .send(HummockEvent::RegisterVectorWriter {
                table_id,
                init_epoch,
            })
            .map_err(|_| HummockError::other("failed to send register vector writer event"))?;
        Ok(Self {
            table_id,
            hummock_event_sender,
        })
    }
}

impl Drop for VectorWriterInitGuard {
    fn drop(&mut self) {
        let _ = self
            .hummock_event_sender
            .send(HummockEvent::DropVectorWriter {
                table_id: self.table_id,
            });
    }
}

struct VectorWriterState {
    epoch: EpochPair,
    writer_impl: VectorWriterImpl,
    _guard: VectorWriterInitGuard,
}

pub struct HummockVectorWriter {
    table_id: TableId,
    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
    sstable_store: SstableStoreRef,
    object_id_manager: ObjectIdManagerRef,
    hummock_event_sender: HummockEventSender,
    storage_opts: Arc<StorageOpts>,

    state: Option<VectorWriterState>,
}

impl HummockVectorWriter {
    pub(super) fn new(
        table_id: TableId,
        version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
        hummock_event_sender: HummockEventSender,
        storage_opts: Arc<StorageOpts>,
    ) -> Self {
        Self {
            table_id,
            version_update_notifier_tx,
            sstable_store,
            object_id_manager,
            hummock_event_sender,
            storage_opts,
            state: None,
        }
    }
}

impl StateStoreWriteEpochControl for HummockVectorWriter {
    async fn init(&mut self, opts: InitOptions) -> StorageResult<()> {
        let version = wait_for_epoch(
            &self.version_update_notifier_tx,
            opts.epoch.prev,
            self.table_id,
        )
        .await?;
        let index = &version.vector_indexes[&self.table_id];
        assert!(
            self.state
                .replace(VectorWriterState {
                    epoch: opts.epoch,
                    writer_impl: VectorWriterImpl::new(
                        index,
                        self.sstable_store.clone(),
                        self.object_id_manager.clone(),
                        &self.storage_opts,
                    ),
                    _guard: VectorWriterInitGuard::new(
                        self.table_id,
                        opts.epoch.curr,
                        self.hummock_event_sender.clone()
                    )?,
                })
                .is_none()
        );
        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, _opts: SealCurrentEpochOptions) {
        let state = self.state.as_mut().expect("should have init");
        let epoch = &mut state.epoch;
        assert!(next_epoch > epoch.curr);
        epoch.prev = epoch.curr;
        epoch.curr = next_epoch;
        let _ = self
            .hummock_event_sender
            .send(HummockEvent::VectorWriterSealEpoch {
                table_id: self.table_id,
                next_epoch,
                add: state.writer_impl.seal_current_epoch(),
            });
    }

    async fn flush(&mut self) -> StorageResult<usize> {
        Ok(self
            .state
            .as_mut()
            .expect("should have init")
            .writer_impl
            .flush()
            .await?)
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        Ok(self
            .state
            .as_mut()
            .expect("should have init")
            .writer_impl
            .try_flush()
            .await?)
    }
}

impl StateStoreWriteVector for HummockVectorWriter {
    fn insert(&mut self, vec: Vector, info: Bytes) -> StorageResult<()> {
        Ok(self
            .state
            .as_mut()
            .expect("should have init")
            .writer_impl
            .insert(vec, info)?)
    }
}
