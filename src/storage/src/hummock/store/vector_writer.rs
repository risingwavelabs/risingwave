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

use std::mem::take;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::vector_index::{
    FlatIndexAdd, VectorFileInfo, VectorIndexAdd, VectorIndexImpl, VectorStoreDelta,
};
use risingwave_hummock_sdk::{HummockEpoch, HummockObjectId};

use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::event_handler::hummock_event_handler::HummockEventSender;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::utils::wait_for_epoch;
use crate::hummock::vector::block::VectorBlockBuilder;
use crate::hummock::{HummockError, HummockResult, ObjectIdManagerRef, SstableStoreRef};
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
    dimension: usize,
    next_vector_id: u64,
    _guard: VectorWriterInitGuard,
}

pub struct HummockVectorWriter {
    table_id: TableId,
    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
    sstable_store: SstableStoreRef,
    object_id_manager: ObjectIdManagerRef,
    hummock_event_sender: HummockEventSender,

    state: Option<VectorWriterState>,
    flushed_vector_files: Vec<VectorFileInfo>,
    block_builder: Option<VectorBlockBuilder>,
}

impl HummockVectorWriter {
    pub(super) fn new(
        table_id: TableId,
        version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
        sstable_store: SstableStoreRef,
        object_id_manager: ObjectIdManagerRef,
        hummock_event_sender: HummockEventSender,
    ) -> Self {
        Self {
            table_id,
            version_update_notifier_tx,
            sstable_store,
            object_id_manager,
            hummock_event_sender,
            state: None,
            flushed_vector_files: vec![],
            block_builder: None,
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
        let VectorIndexImpl::Flat(flat) = &index.inner;
        assert!(
            self.state
                .replace(VectorWriterState {
                    epoch: opts.epoch,
                    dimension: index.dimension as _,
                    next_vector_id: flat.vector_store.next_vector_id,
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
        assert!(self.block_builder.is_none());
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
                add: VectorIndexAdd::Flat(FlatIndexAdd {
                    vector_store_delta: VectorStoreDelta {
                        next_vector_id: state.next_vector_id,
                        added_vector_files: take(&mut self.flushed_vector_files),
                    },
                }),
            });
    }

    async fn flush(&mut self) -> StorageResult<usize> {
        if let Some(builder) = self.block_builder.take()
            && let Some(block) = builder.finish()
        {
            let vector_count = block.count();
            // TODO: use with_capacity
            let mut encoded_block = BytesMut::new();
            block.encode(&mut encoded_block);
            let encoded_block = encoded_block.freeze();
            let size = encoded_block.len();
            let object_id = self.object_id_manager.get_new_object_id().await?;
            // TODO: may not store with the sst. consider how to manage GC
            let path = self
                .sstable_store
                .get_object_data_path(HummockObjectId::VectorFile(object_id));
            self.sstable_store
                .store()
                .upload(&path, encoded_block)
                .await
                .map_err(HummockError::from)?;
            let state = self.state.as_mut().expect("should have init");

            self.flushed_vector_files.push(VectorFileInfo {
                object_id,
                vector_count: vector_count as _,
                file_size: size as _,
                start_vector_id: state.next_vector_id,
            });
            state.next_vector_id += vector_count as u64;
            Ok(size)
        } else {
            Ok(0)
        }
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        // TODO: flush when the buffer is full
        Ok(())
    }
}

impl StateStoreWriteVector for HummockVectorWriter {
    fn insert(&mut self, vec: Vector, info: Bytes) -> StorageResult<()> {
        let builder = self.block_builder.get_or_insert_with(|| {
            VectorBlockBuilder::new(self.state.as_ref().expect("should have init").dimension)
        });
        builder.add(vec.to_ref(), &info);
        Ok(())
    }
}
