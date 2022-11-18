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
use risingwave_hummock_sdk::key::{map_table_key_range, FullKey, TableKey, TableKeyRange};
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
#[cfg(any(test, feature = "test"))]
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::store::version::{read_filter_for_local, HummockVersionReader};
use crate::hummock::{MemoryLimiter, SstableIterator};
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

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,
}

pub struct LocalHummockStorage {
    core: Arc<HummockStorageCore>,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,
}

// Clone is only used for unit test
#[cfg(any(test, feature = "test"))]
impl Clone for LocalHummockStorage {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            #[cfg(not(madsim))]
            tracing: self.tracing.clone(),
        }
    }
}

impl HummockStorageCore {
    pub fn new(
        read_version: Arc<RwLock<HummockReadVersion>>,
        hummock_version_reader: HummockVersionReader,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
    ) -> Self {
        Self {
            read_version,
            event_sender,
            memory_limiter,
            hummock_version_reader,
        }
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.read_version.write().update(info)
    }

    pub async fn get_inner<'a>(
        &'a self,
        table_key: TableKey<&'a [u8]>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let table_key_range = (
            Bound::Included(TableKey(table_key.to_vec())),
            Bound::Included(TableKey(table_key.to_vec())),
        );

        let read_snapshot = read_filter_for_local(
            epoch,
            read_options.table_id,
            &table_key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .get(table_key, epoch, read_options, read_snapshot)
            .await
    }

    pub async fn iter_inner(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let read_snapshot = read_filter_for_local(
            epoch,
            read_options.table_id,
            &table_key_range,
            self.read_version.clone(),
        )?;

        self.hummock_version_reader
            .iter(table_key_range, epoch, read_options, read_snapshot)
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
        self.core.get_inner(TableKey(key), epoch, read_options)
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        let iter = self
            .core
            .iter_inner(map_table_key_range(key_range), epoch, read_options);
        #[cfg(not(madsim))]
        return iter.in_span(self.tracing.new_tracer("hummock_iter"));
        #[cfg(madsim)]
        iter
    }
}

impl StateStoreWrite for LocalHummockStorage {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            if kv_pairs.is_empty() && delete_ranges.is_empty() {
                return Ok(0);
            }

            let epoch = write_options.epoch;
            let table_id = write_options.table_id;

            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                kv_pairs,
                delete_ranges,
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
        sstable_store: SstableStoreRef,
        read_version: Arc<RwLock<HummockReadVersion>>,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
    ) -> Self {
        Self::new(
            read_version,
            HummockVersionReader::new(sstable_store, Arc::new(StateStoreMetrics::unused())),
            event_sender,
            MemoryLimiter::unlimit(),
            #[cfg(not(madsim))]
            Arc::new(risingwave_tracing::RwTracingService::new()),
        )
    }

    pub fn new(
        read_version: Arc<RwLock<HummockReadVersion>>,
        hummock_version_reader: HummockVersionReader,
        event_sender: mpsc::UnboundedSender<HummockEvent>,
        memory_limiter: Arc<MemoryLimiter>,
        #[cfg(not(madsim))] tracing: Arc<risingwave_tracing::RwTracingService>,
    ) -> Self {
        let storage_core = HummockStorageCore::new(
            read_version,
            hummock_version_reader,
            event_sender,
            memory_limiter,
        );

        Self {
            core: Arc::new(storage_core),
            #[cfg(not(madsim))]
            tracing,
        }
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.core.update(info)
    }

    pub fn read_version(&self) -> Arc<RwLock<HummockReadVersion>> {
        self.core.read_version.clone()
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
    type Item = (FullKey<Vec<u8>>, Bytes);

    type NextFuture<'a> = impl Future<Output = StorageResult<Option<Self::Item>>> + Send + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            let iter = &mut self.inner;

            if iter.is_valid() {
                let kv = (iter.key().clone(), Bytes::copy_from_slice(iter.value()));
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
