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

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc;

use super::version::{HummockReadVersion, StagingData, VersionUpdate};
use super::{
    GetFutureTrait, HummockStorageIterator, IngestKVBatchFutureTrait, IterFutureTrait, ReadOptions,
    StateStore, WriteOptions,
};
use crate::define_local_state_store_associated_type;
use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::store::version::{read_filter_for_local, HummockVersionReader};
use crate::hummock::{HummockResult, MemoryLimiter, SstableIdManager, SstableIdManagerRef};
use crate::monitor::StateStoreMetrics;
use crate::storage_value::StorageValue;

#[expect(dead_code)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    // memtable: Memtable,

    /// Read handle.
    read_version: Arc<RwLock<HummockReadVersion>>,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,

    options: Arc<StorageConfig>,

    sstable_id_manager: SstableIdManagerRef,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,
}

#[derive(Clone)]
pub struct HummockStorage {
    core: Arc<HummockStorageCore>,
}

impl HummockStorageCore {
    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            Arc::new(StateStoreMetrics::unused()),
            read_version,
            event_sender,
            MemoryLimiter::unlimit(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
    ) -> HummockResult<Self> {
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));

        let instance = Self {
            options,
            read_version,
            hummock_meta_client,
            sstable_store: sstable_store.clone(),
            stats: stats.clone(),
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing: Arc::new(risingwave_tracing::RwTracingService::new()),
            event_sender,
            memory_limiter,
            hummock_version_reader: HummockVersionReader::new(sstable_store, stats),
        };
        Ok(instance)
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.read_version.write().update(info)
    }

    pub async fn get_inner<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let key_range = (Bound::Included(key.to_vec()), Bound::Included(key.to_vec()));

        let read_snapshot = read_filter_for_local(
            epoch,
            read_options.table_id,
            &key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .get(key, epoch, read_options, read_snapshot)
            .await
    }

    pub async fn iter_inner(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let read_snapshot = read_filter_for_local(
            epoch,
            read_options.table_id,
            &key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .iter(key_range, epoch, read_options, read_snapshot)
            .await
    }
}

#[expect(unused_variables)]
impl StateStore for HummockStorage {
    type Iter = HummockStorageIterator;

    define_local_state_store_associated_type!();

    fn insert(&self, key: Bytes, val: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn delete(&self, key: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move { self.core.get_inner(key, epoch, read_options).await }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move { self.core.iter_inner(key_range, epoch, read_options).await }
    }

    fn flush(&self) -> StorageResult<usize> {
        unimplemented!()
    }

    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()> {
        unimplemented!()
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestKVBatchFuture<'_> {
        async move {
            let epoch = write_options.epoch;
            let table_id = write_options.table_id;

            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                kv_pairs,
                table_id,
                Some(self.core.memory_limiter.as_ref()),
            )
            .await;
            let imm_size = imm.size();
            self.core
                .update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

            // insert imm to uploader
            self.core
                .event_sender
                .send(HummockEvent::ImmToUploader(imm))
                .unwrap();

            Ok(imm_size)
        }
    }
}

impl HummockStorage {
    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::for_test(
            options,
            sstable_store,
            hummock_meta_client,
            read_version,
            event_sender,
        )?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::new(
            options,
            sstable_store,
            hummock_meta_client,
            stats,
            read_version,
            event_sender,
            memory_limiter,
        )?;

        let instance = Self {
            core: Arc::new(storage_core),
        };
        Ok(instance)
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.core.update(info)
    }

    pub fn read_version(&self) -> Arc<RwLock<HummockReadVersion>> {
        self.core.read_version.clone()
    }
}
