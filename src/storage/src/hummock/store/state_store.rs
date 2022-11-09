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
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
#[cfg(not(madsim))]
use minitrace::future::FutureExt;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc;

use super::version::{HummockReadVersion, StagingData, VersionUpdate};
use crate::error::StorageResult;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::iterator::{
    ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::store::version::{read_filter_for_local, HummockVersionReader};
use crate::hummock::{
    HummockResult, MemoryLimiter, SstableIdManager, SstableIdManagerRef, SstableIterator,
};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::store::{
    GetFutureTrait, IngestBatchFutureTrait, IterFutureTrait, LocalStateStore, ReadOptions,
    StateStoreRead, StateStoreWrite, WriteOptions,
};
use crate::{
    define_state_store_read_associated_type, define_state_store_write_associated_type,
    StateStoreIter,
};

pub struct HummockStorageCore {
    /// Mutable memtable.
    // memtable: Memtable,

    /// Read handle.
    read_version: Arc<RwLock<HummockReadVersion>>,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    options: Arc<StorageConfig>,

    sstable_id_manager: SstableIdManagerRef,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,
}

#[derive(Clone)]
pub struct LocalHummockStorage {
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
        sstable_id_manager: Arc<SstableIdManager>,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            Arc::new(StateStoreMetrics::unused()),
            read_version,
            event_sender,
            MemoryLimiter::unlimit(),
            sstable_id_manager,
            #[cfg(not(madsim))]
            Arc::new(risingwave_tracing::RwTracingService::new()),
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
        sstable_id_manager: Arc<SstableIdManager>,
        #[cfg(not(madsim))] tracing: Arc<risingwave_tracing::RwTracingService>,
    ) -> HummockResult<Self> {
        let instance = Self {
            read_version,
            event_sender,
            hummock_meta_client,
            sstable_store: sstable_store.clone(),
            options,
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing,
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

impl StateStoreRead for LocalHummockStorage {
    type Iter = HummockStorageIterator;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        self.core.get_inner(key, epoch, read_options)
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        let iter = self.core.iter_inner(key_range, epoch, read_options);
        #[cfg(not(madsim))]
        return iter.in_span(self.core.tracing.new_tracer("hummock_iter"));
        #[cfg(madsim)]
        iter
    }
}

impl StateStoreWrite for LocalHummockStorage {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            if kv_pairs.is_empty() {
                return Ok(0);
            }

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

impl LocalStateStore for LocalHummockStorage {}

impl LocalHummockStorage {
    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        sstable_id_manager: Arc<SstableIdManager>,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::for_test(
            options,
            sstable_store,
            hummock_meta_client,
            read_version,
            event_sender,
            sstable_id_manager,
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
        sstable_id_manager: Arc<SstableIdManager>,
        #[cfg(not(madsim))] tracing: Arc<risingwave_tracing::RwTracingService>,
    ) -> HummockResult<Self> {
        let storage_core = HummockStorageCore::new(
            options,
            sstable_store,
            hummock_meta_client,
            stats,
            read_version,
            event_sender,
            memory_limiter,
            sstable_id_manager,
            #[cfg(not(madsim))]
            tracing,
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

    pub fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.core.memory_limiter.clone()
    }

    pub fn hummock_meta_client(&self) -> &Arc<dyn HummockMetaClient> {
        &self.core.hummock_meta_client
    }

    pub fn options(&self) -> &Arc<StorageConfig> {
        &self.core.options
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.core.sstable_store.clone()
    }

    pub fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        &self.core.sstable_id_manager
    }
}

pub type StagingDataIterator = OrderedMergeIteratorInner<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;
type HummockStorageIteratorPayload = UnorderedMergeIteratorInner<
    HummockIteratorUnion<
        Forward,
        StagingDataIterator,
        OrderedMergeIteratorInner<SstableIterator>,
        ConcatIteratorInner<SstableIterator>,
    >,
>;

pub struct HummockStorageIterator {
    inner: UserIterator<HummockStorageIteratorPayload>,
    metrics: Arc<StateStoreMetrics>,
}

impl StateStoreIter for HummockStorageIterator {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> = impl Future<Output = StorageResult<Option<Self::Item>>> + Send + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
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

impl HummockStorageIterator {
    pub fn new(
        inner: UserIterator<HummockStorageIteratorPayload>,
        metrics: Arc<StateStoreMetrics>,
    ) -> Self {
        Self { inner, metrics }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats);
    }
}

impl Drop for HummockStorageIterator {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic(&mut stats);
        stats.report(&self.metrics);
    }
}
