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

use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc;

use super::event_handler::HummockEvent;
// use crate::table::streaming_table::mem_table::MemTable;
use super::memtable::{BTreeMapMemtable, Memtable};
use super::version::{HummockReadVersion, StagingData, VersionUpdate};
use super::{
    GetFutureTrait, IngestKVBatchFutureTrait, IterFutureTrait, ReadOptions, StateStore,
    WriteOptions,
};
use crate::define_local_state_store_associated_type;
use crate::error::StorageResult;
// use super::monitor::StateStoreMetrics;
use crate::hummock::compaction_group_client::{
    CompactionGroupClientImpl, DummyCompactionGroupClient,
};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferItem};
use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::{build_shared_buffer_item_batches, HummockResult, HummockStateStoreIter};
use crate::storage_value::StorageValue;

#[allow(unused)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    memtable: Memtable,

    /// Read handle.
    read_version: Mutex<HummockReadVersion>,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    local_version_manager: Arc<LocalVersionManager>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Statistics
    // stats: Arc<StateStoreMetrics>,
    compaction_group_client: Arc<CompactionGroupClientImpl>,
    // sstable_id_manager: SstableIdManagerRef,
}

#[allow(unused)]
#[derive(Clone)]
pub struct HummockStorage {
    core: Arc<HummockStorageCore>,
}

#[allow(unused)]
impl HummockStorageCore {
    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) -> HummockResult<()> {
        let mut read_version = self.read_version.lock().unwrap();
        read_version.update(info)
    }
}

#[allow(unused)]
impl StateStore for HummockStorage {
    type Iter = HummockStateStoreIter;

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
        async move {
            let read_version_snapshot = self.core.read_version.lock().unwrap().read_filter(
                epoch,
                &(key..=key),
                &read_options,
            );

            read_version_snapshot.get(key, epoch, read_options).await
        }
    }

    fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn flush(&self) -> StorageResult<usize> {
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
            // let storage_core = &mut self.core.lock().unwrap();
            let uploader = self.core.local_version_manager.clone();

            // TODO: get compaction_group_id
            let compaction_group_id = 2;

            let imm = uploader.build_shared_buffer_batch(
                epoch,
                compaction_group_id,
                kv_pairs,
                table_id.into(),
            );
            let imm_size = imm.size();
            self.core
                .update(VersionUpdate::Staging(StagingData::ImmMem(Arc::new(
                    imm.clone(),
                ))));

            // insert imm to upload
            uploader.write_shared_buffer_batch(imm)?;
            Ok(imm_size)
        }
    }

    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()> {
        unimplemented!()
    }
}
