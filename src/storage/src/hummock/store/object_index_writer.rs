// Copyright 2026 RisingWave Labs
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

//! Shared part of the writers of object indexes, i.e. the indexes whose data is stored in their
//! own objects instead of SSTs.
//!
//! [`HummockObjectIndexWriter`] handles epoch control and the registration to the uploader, and
//! delegates the index-specific work to an [`ObjectIndexWriterImpl`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::vector_index::VectorIndexAdd;
use risingwave_hummock_sdk::{HummockEpoch, HummockRawObjectId, ObjectIndexAdd};

use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::event_handler::hummock_event_handler::HummockEventSender;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::utils::wait_for_epoch;
use crate::hummock::{
    HummockError, HummockResult, ObjectIdManager, ObjectIdManagerRef, SstableStoreRef,
};
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;
use crate::store::{InitOptions, SealCurrentEpochOptions};

/// Allocates object ids for the files of object indexes.
#[async_trait::async_trait]
pub trait ObjectIndexFileIdManager: Send + Sync {
    async fn get_new_file_object_id(&self) -> HummockResult<HummockRawObjectId>;
    /// Releases an unsealed file that will not be included in an index update.
    fn discard_unsealed_file(&self, id: HummockRawObjectId);
    /// Gives a background task independent ownership of its unsealed uploads.
    fn new_upload_scope(self: Arc<Self>) -> ObjectIndexFileIdManagerRef;
}

pub type ObjectIndexFileIdManagerRef = Arc<dyn ObjectIndexFileIdManager>;

static NEXT_WRITER_ID: AtomicU64 = AtomicU64::new(0);

#[async_trait::async_trait]
impl ObjectIndexFileIdManager for ObjectIdManager {
    async fn get_new_file_object_id(&self) -> HummockResult<HummockRawObjectId> {
        self.get_new_object_id().await
    }

    fn discard_unsealed_file(&self, _id: HummockRawObjectId) {}

    fn new_upload_scope(self: Arc<Self>) -> ObjectIndexFileIdManagerRef {
        Arc::new(TrackedObjectIndexFileIdManager {
            inner: self,
            owner: Arc::new(()),
        })
    }
}

/// The owner is also retained by background tasks through their cloned allocator. Register
/// each ID before it can be uploaded, not only when the writer eventually seals its epoch.
struct TrackedObjectIndexFileIdManager {
    inner: ObjectIdManagerRef,
    owner: Arc<()>,
}

#[async_trait::async_trait]
impl ObjectIndexFileIdManager for TrackedObjectIndexFileIdManager {
    async fn get_new_file_object_id(&self) -> HummockResult<HummockRawObjectId> {
        let id = self.inner.get_new_object_id().await?;
        self.inner.object_index_uploads().track(id, &self.owner);
        Ok(id)
    }

    fn discard_unsealed_file(&self, id: HummockRawObjectId) {
        self.inner
            .object_index_uploads()
            .discard_unsealed(id, &self.owner);
    }

    fn new_upload_scope(self: Arc<Self>) -> ObjectIndexFileIdManagerRef {
        Arc::new(Self {
            inner: self.inner.clone(),
            owner: Arc::new(()),
        })
    }
}

fn added_object_ids(add: &ObjectIndexAdd) -> Vec<HummockRawObjectId> {
    match add {
        ObjectIndexAdd::Vector(add) => {
            let (delta, graph_id) = match add {
                VectorIndexAdd::Flat(add) => (&add.vector_store_info_delta, None),
                VectorIndexAdd::HnswFlat(add) => (
                    &add.vector_store_info_delta,
                    Some(add.graph_file.object_id.as_raw()),
                ),
            };
            delta
                .added_vector_files
                .iter()
                .map(|file| file.object_id.as_raw())
                .chain(graph_id)
                .collect()
        }
    }
}

pub(crate) struct ObjectIndexWriterContext {
    pub table_id: TableId,
    pub sstable_store: SstableStoreRef,
    pub object_id_manager: ObjectIdManagerRef,
    pub stats: Arc<HummockStateStoreMetrics>,
    pub storage_opts: Arc<StorageOpts>,
}

/// Index-specific part of an object index writer.
pub(crate) trait ObjectIndexWriterImpl: Sized + Send + Sync + 'static {
    /// Creates the writer on top of the committed index in `version`. `init_epoch` is the first
    /// epoch to write.
    fn create<'a>(
        ctx: &'a ObjectIndexWriterContext,
        version: &'a PinnedVersion,
        init_epoch: HummockEpoch,
        file_id_manager: ObjectIndexFileIdManagerRef,
    ) -> impl Future<Output = HummockResult<Self>> + Send + 'a;

    /// Seals the current epoch and returns the changes to commit with it, if any.
    fn seal_current_epoch(
        &mut self,
        next_epoch: HummockEpoch,
        version: &PinnedVersion,
    ) -> Option<ObjectIndexAdd>;

    fn flush(&mut self) -> impl Future<Output = HummockResult<usize>> + Send + '_;

    fn try_flush(&mut self) -> impl Future<Output = HummockResult<()>> + Send + '_;
}

/// Registers the writer to the uploader on creation and deregisters it on drop.
struct ObjectIndexWriterInitGuard {
    table_id: TableId,
    writer_id: u64,
    hummock_event_sender: HummockEventSender,
}

impl ObjectIndexWriterInitGuard {
    fn new(
        table_id: TableId,
        init_epoch: HummockEpoch,
        hummock_event_sender: HummockEventSender,
    ) -> HummockResult<Self> {
        let writer_id = NEXT_WRITER_ID.fetch_add(1, Ordering::Relaxed);
        hummock_event_sender
            .send(HummockEvent::RegisterObjectIndexWriter {
                table_id,
                writer_id,
                init_epoch,
            })
            .map_err(|_| {
                HummockError::other("failed to send register object index writer event")
            })?;
        Ok(Self {
            table_id,
            writer_id,
            hummock_event_sender,
        })
    }
}

impl Drop for ObjectIndexWriterInitGuard {
    fn drop(&mut self) {
        let _ = self
            .hummock_event_sender
            .send(HummockEvent::DropObjectIndexWriter {
                table_id: self.table_id,
                writer_id: self.writer_id,
            });
    }
}

struct ObjectIndexWriterState<W> {
    epoch: EpochPair,
    writer_impl: W,
    _guard: ObjectIndexWriterInitGuard,
}

pub(crate) struct HummockObjectIndexWriter<W> {
    ctx: ObjectIndexWriterContext,
    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
    hummock_event_sender: HummockEventSender,

    state: Option<ObjectIndexWriterState<W>>,
}

impl<W: ObjectIndexWriterImpl> HummockObjectIndexWriter<W> {
    pub(crate) fn new(
        ctx: ObjectIndexWriterContext,
        version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
        hummock_event_sender: HummockEventSender,
    ) -> Self {
        Self {
            ctx,
            version_update_notifier_tx,
            hummock_event_sender,
            state: None,
        }
    }

    pub(crate) fn writer_impl(&mut self) -> &mut W {
        &mut self.state.as_mut().expect("should have init").writer_impl
    }

    pub(crate) async fn init(&mut self, opts: InitOptions) -> StorageResult<()> {
        let version = wait_for_epoch(
            &self.version_update_notifier_tx,
            opts.epoch.prev,
            self.ctx.table_id,
        )
        .await?;
        let file_id_manager = self.ctx.object_id_manager.clone().new_upload_scope();
        let writer_impl = W::create(&self.ctx, &version, opts.epoch.curr, file_id_manager).await?;
        assert!(
            self.state
                .replace(ObjectIndexWriterState {
                    epoch: opts.epoch,
                    writer_impl,
                    _guard: ObjectIndexWriterInitGuard::new(
                        self.ctx.table_id,
                        opts.epoch.curr,
                        self.hummock_event_sender.clone()
                    )?,
                })
                .is_none()
        );
        Ok(())
    }

    pub(crate) fn seal_current_epoch(&mut self, next_epoch: u64, _opts: SealCurrentEpochOptions) {
        let state = self.state.as_mut().expect("should have init");
        let epoch = &mut state.epoch;
        assert!(next_epoch > epoch.curr);
        epoch.prev = epoch.curr;
        epoch.curr = next_epoch;
        let add = state
            .writer_impl
            .seal_current_epoch(next_epoch, &self.version_update_notifier_tx.borrow());
        if let Some(add) = &add {
            self.ctx.object_id_manager.object_index_uploads().seal(
                self.ctx.table_id,
                epoch.prev,
                added_object_ids(add),
            );
        }
        let _ = self
            .hummock_event_sender
            .send(HummockEvent::ObjectIndexWriterSealEpoch {
                table_id: self.ctx.table_id,
                writer_id: state._guard.writer_id,
                next_epoch,
                add,
            });
    }

    pub(crate) async fn flush(&mut self) -> StorageResult<usize> {
        Ok(self.writer_impl().flush().await?)
    }

    pub(crate) async fn try_flush(&mut self) -> StorageResult<()> {
        Ok(self.writer_impl().try_flush().await?)
    }
}
