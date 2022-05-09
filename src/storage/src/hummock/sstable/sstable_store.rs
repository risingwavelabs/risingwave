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

use std::clone::Clone;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use fail::fail_point;
use risingwave_hummock_sdk::{is_remote_sst_id, HummockSSTableId};

use crate::hummock::{
    Block, BlockCache, BlockHolder, CachableEntry, HummockError, HummockResult, LookupResult,
    LruCache, Sstable, SstableMeta,
};
use crate::monitor::StateStoreMetrics;
use crate::object::{BlockLocation, BoxedObjectUploader, ObjectStoreRef};

const DEFAULT_META_CACHE_SHARD_BITS: usize = 5;
const DEFAULT_META_CACHE_OBJECT_POOL_CAPACITY: usize = 16;
pub type TableHolder = CachableEntry<HummockSSTableId, Box<Sstable>>;

pub type MetaCache = Arc<LruCache<HummockSSTableId, Box<Sstable>>>;

#[derive(Clone, Copy)]
pub enum ReadCachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill,
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

#[derive(Clone, Copy)]
pub enum WriteCachePolicy {
    /// Never write to the cache.
    Disable,
    /// Always write to the cache even the whole SST build may not succeed.
    FillAny,
    /// Write to the cache only if the whole SST build succeeds.
    FillOnSuccess,
}

pub struct SstableWriter {
    sst_id: HummockSSTableId,
    data_uploader: BoxedObjectUploader,
    meta_uploader: BoxedObjectUploader,
    cache_policy: WriteCachePolicy,
    block_cache: BlockCache,
    meta_cache: MetaCache,
    written_len: usize,
    block_count: u32,
}

impl SstableWriter {
    pub fn new(
        sst_id: HummockSSTableId,
        data_uploader: BoxedObjectUploader,
        meta_uploader: BoxedObjectUploader,
        cache_policy: WriteCachePolicy,
        block_cache: BlockCache,
        meta_cache: MetaCache,
    ) -> Self {
        Self {
            sst_id,
            data_uploader,
            meta_uploader,
            cache_policy,
            block_cache,
            meta_cache,
            written_len: 0,
            block_count: 0,
        }
    }

    pub fn written_len(&self) -> usize {
        self.written_len
    }

    pub fn get_sst_id(&self) -> &HummockSSTableId {
        &self.sst_id
    }

    pub async fn write_block(&mut self, data: Bytes) -> HummockResult<()> {
        self.data_uploader
            .upload(data.as_ref())
            .await
            .map_err(HummockError::object_io_error)?;
        self.written_len += data.len();
        let block_idx = self.block_count as u64;
        self.block_count += 1;
        if let WriteCachePolicy::FillAny = self.cache_policy {
            self.block_cache.insert(
                self.sst_id,
                block_idx,
                Box::new(
                    Block::decode(data).expect("the written block should be able to be decoded"),
                ),
            );
        }
        Ok(())
    }

    pub async fn finish(mut self, meta: &SstableMeta) -> HummockResult<()> {
        // Upload the remaining size footer.
        self.data_uploader
            .upload(self.block_count.to_le_bytes().as_ref())
            .await
            .map_err(HummockError::object_io_error)?;

        // Finish uploading the data and try to add to block cache.
        match self.cache_policy {
            WriteCachePolicy::FillOnSuccess => {
                let mut stream = self
                    .data_uploader
                    .finish_with_data()
                    .await
                    .map_err(HummockError::object_io_error)?;
                for (block_idx, block_meta) in meta.block_metas.iter().enumerate() {
                    // Try to add each block to block cache. Error will be ignored.
                    match stream.read(block_meta.len as usize).await {
                        Ok(block_data) => match Block::decode(Bytes::copy_from_slice(block_data)) {
                            Ok(block) => {
                                self.block_cache.insert(
                                    self.sst_id,
                                    block_idx as u64,
                                    Box::new(block),
                                );
                            }
                            Err(e) => {
                                tracing::error!("failed to decode block data to add to block cache: {:?}. ignored.", e);
                            }
                        },
                        Err(e) => {
                            tracing::error!(
                                "Failed to read block data to add to block cache: {:?}. ignored",
                                e
                            );
                        }
                    }
                }
            }
            _ => {
                self.data_uploader
                    .finish()
                    .await
                    .map_err(HummockError::object_io_error)?;
            }
        }

        // Upload the meta.
        let meta_bytes = meta.encode_to_bytes();
        self.meta_uploader
            .upload(meta_bytes.as_ref())
            .await
            .map_err(HummockError::object_io_error)?;
        self.meta_uploader
            .finish()
            .await
            .map_err(HummockError::object_io_error)?;

        match self.cache_policy {
            WriteCachePolicy::FillOnSuccess | WriteCachePolicy::FillAny => {
                self.meta_cache.insert(
                    self.sst_id,
                    self.sst_id,
                    meta_bytes.len(),
                    Box::new(Sstable {
                        id: self.sst_id,
                        meta: meta.clone(),
                    }),
                );
            }
            _ => {}
        }
        Ok(())
    }
}

pub struct SstableStore {
    path: String,
    remote_store: ObjectStoreRef,
    local_store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: MetaCache,
    /// Statistics.
    stats: Arc<StateStoreMetrics>,
    next_local_sst_id: AtomicU64,
}

impl SstableStore {
    pub fn new(
        remote_store: ObjectStoreRef,
        local_store: ObjectStoreRef,
        path: String,
        stats: Arc<StateStoreMetrics>,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
    ) -> Self {
        let meta_cache = Arc::new(LruCache::new(
            DEFAULT_META_CACHE_SHARD_BITS,
            meta_cache_capacity,
            DEFAULT_META_CACHE_OBJECT_POOL_CAPACITY,
        ));
        Self {
            path,
            remote_store,
            local_store,
            block_cache: BlockCache::new(block_cache_capacity),
            meta_cache,
            stats,
            next_local_sst_id: AtomicU64::new(0),
        }
    }

    pub async fn new_sstable_writer(
        &self,
        sst_id: HummockSSTableId,
        cache_policy: WriteCachePolicy,
    ) -> HummockResult<SstableWriter> {
        let object_store = if is_remote_sst_id(sst_id) {
            self.remote_store.clone()
        } else {
            self.local_store.clone()
        };

        let data_uploader = object_store
            .get_upload_handle(&self.get_sst_data_path(sst_id))
            .await
            .map_err(HummockError::object_io_error)?;
        let meta_uploader = object_store
            .get_upload_handle(&self.get_sst_meta_path(sst_id))
            .await
            .map_err(HummockError::object_io_error)?;

        Ok(SstableWriter::new(
            sst_id,
            data_uploader,
            meta_uploader,
            cache_policy,
            self.block_cache.clone(),
            self.meta_cache.clone(),
        ))
    }

    pub async fn put(
        &self,
        sst: &Sstable,
        data: Bytes,
        policy: WriteCachePolicy,
    ) -> HummockResult<()> {
        let timer = self.stats.sst_store_put_remote_duration.start_timer();

        self.put_sst_data(sst.id, data.clone()).await?;

        fail_point!("metadata_upload_err");
        if let Err(e) = self.put_meta(sst).await {
            self.delete_sst_data(sst.id).await?;
            return Err(e);
        }

        timer.observe_duration();

        match policy {
            WriteCachePolicy::FillOnSuccess | WriteCachePolicy::FillAny => {
                // TODO: use concurrent put object
                for (block_idx, meta) in sst.meta.block_metas.iter().enumerate() {
                    let offset = meta.offset as usize;
                    let len = meta.len as usize;
                    self.add_block_cache(
                        sst.id,
                        block_idx as u64,
                        data.slice(offset..offset + len),
                    )
                    .await
                    .unwrap();
                }
                self.meta_cache
                    .insert(sst.id, sst.id, sst.encoded_size(), Box::new(sst.clone()));
            }
            _ => {}
        };

        Ok(())
    }

    pub async fn put_meta(&self, sst: &Sstable) -> HummockResult<()> {
        let meta_path = self.get_sst_meta_path(sst.id);
        let meta = Bytes::from(sst.meta.encode_to_bytes());
        self.get_store_of_table(sst.id)
            .upload(&meta_path, meta)
            .await
            .map_err(HummockError::object_io_error)
    }

    pub async fn put_sst_data(&self, sst_id: HummockSSTableId, data: Bytes) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(sst_id);
        self.get_store_of_table(sst_id)
            .upload(&data_path, data)
            .await
            .map_err(HummockError::object_io_error)
    }

    pub async fn delete_sst_data(&self, sst_id: HummockSSTableId) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(sst_id);
        self.get_store_of_table(sst_id)
            .delete(&data_path)
            .await
            .map_err(HummockError::object_io_error)
    }

    async fn add_block_cache(
        &self,
        sst_id: HummockSSTableId,
        block_idx: u64,
        block_data: Bytes,
    ) -> HummockResult<()> {
        let block = Box::new(Block::decode(block_data)?);
        self.block_cache.insert(sst_id, block_idx, block);
        Ok(())
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: u64,
        policy: ReadCachePolicy,
    ) -> HummockResult<BlockHolder> {
        self.stats.sst_store_block_request_counts.inc();

        let fetch_block = async move {
            let timer = self.stats.sst_store_get_remote_duration.start_timer();

            let block_meta = sst
                .meta
                .block_metas
                .get(block_index as usize)
                .ok_or_else(HummockError::invalid_block)?;
            let block_loc = BlockLocation {
                offset: block_meta.offset as usize,
                size: block_meta.len as usize,
            };
            let data_path = self.get_sst_data_path(sst.id);
            let block_data = self
                .get_store_of_table(sst.id)
                .read(&data_path, Some(block_loc))
                .await
                .map_err(HummockError::object_io_error)?;
            let block = Block::decode(block_data)?;

            timer.observe_duration();
            Ok(Box::new(block))
        };

        // In case of failpoints tests, we disable reading from cache
        let policy = if fail::has_failpoints() {
            ReadCachePolicy::Disable
        } else {
            policy
        };

        match policy {
            ReadCachePolicy::Fill => {
                self.block_cache
                    .get_or_insert_with(sst.id, block_index, fetch_block)
                    .await
            }
            ReadCachePolicy::NotFill => match self.block_cache.get(sst.id, block_index) {
                Some(block) => Ok(block),
                None => fetch_block.await.map(BlockHolder::from_owned_block),
            },
            ReadCachePolicy::Disable => fetch_block.await.map(BlockHolder::from_owned_block),
        }
    }

    pub async fn sstable(&self, sst_id: HummockSSTableId) -> HummockResult<TableHolder> {
        match self.meta_cache.lookup_for_request(sst_id, sst_id) {
            LookupResult::Cached(entry) => Ok(entry),
            LookupResult::WaitPendingRequest(recv) => recv.await.map_err(HummockError::other),
            LookupResult::Miss => {
                let path = self.get_sst_meta_path(sst_id);
                match self
                    .get_store_of_table(sst_id)
                    .read(&path, None)
                    .await
                    .map_err(HummockError::object_io_error)
                {
                    Ok(buf) => {
                        let meta = SstableMeta::decode(&mut &buf[..])?;
                        let sst = Box::new(Sstable { id: sst_id, meta });
                        let handle =
                            self.meta_cache
                                .insert(sst_id, sst_id, sst.encoded_size(), sst);
                        Ok(handle)
                    }
                    Err(e) => {
                        self.meta_cache.clear_pending_request(&sst_id, sst_id);
                        Err(e)
                    }
                }
            }
        }
    }

    pub fn get_sst_meta_path(&self, sst_id: HummockSSTableId) -> String {
        format!("{}/{}.meta", self.path, sst_id)
    }

    pub fn get_sst_data_path(&self, sst_id: HummockSSTableId) -> String {
        format!("{}/{}.data", self.path, sst_id)
    }

    pub fn remote_store(&self) -> ObjectStoreRef {
        self.remote_store.clone()
    }

    pub fn local_store(&self) -> ObjectStoreRef {
        self.local_store.clone()
    }

    pub fn get_next_local_sst_id(&self) -> HummockSSTableId {
        self.next_local_sst_id.fetch_add(1, Relaxed)
    }

    fn get_store_of_table(&self, sst_id: HummockSSTableId) -> &ObjectStoreRef {
        if is_remote_sst_id(sst_id) {
            &self.remote_store
        } else {
            &self.local_store
        }
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
