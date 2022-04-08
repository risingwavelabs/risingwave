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

use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;

use super::{Block, BlockCache, Sstable, SstableMeta};
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::StateStoreMetrics;
use crate::object::{BlockLocation, ObjectStoreRef};

const DEFAULT_META_CACHE_INIT_CAPACITY: usize = 1024;

// TODO: Define policy based on use cases (read / compaction / ...).
pub enum CachePolicy {
    Disable,
    Fill,
    NotFill,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Cache<u64, Arc<Sstable>>,
    /// Statistics.
    stats: Arc<StateStoreMetrics>,
}

impl SstableStore {
    pub fn new(
        store: ObjectStoreRef,
        path: String,
        stats: Arc<StateStoreMetrics>,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
    ) -> Self {
        let meta_cache: Cache<u64, Arc<Sstable>> = Cache::builder()
            .weigher(|_k, v: &Arc<Sstable>| v.encoded_size() as u32)
            .initial_capacity(DEFAULT_META_CACHE_INIT_CAPACITY)
            .max_capacity(meta_cache_capacity as u64)
            .build();

        Self {
            path,
            store,
            block_cache: BlockCache::new(block_cache_capacity),
            meta_cache,
            stats,
        }
    }

    pub async fn put(
        &self,
        sst: &Sstable,
        data: Bytes,
        policy: CachePolicy,
    ) -> HummockResult<usize> {
        let timer = self.stats.sst_store_put_remote_duration.start_timer();

        let meta = Bytes::from(sst.meta.encode_to_bytes());
        let len = data.len();

        let data_path = self.get_sst_data_path(sst.id);
        self.store
            .upload(&data_path, data.clone())
            .await
            .map_err(HummockError::object_io_error)?;

        let meta_path = self.get_sst_meta_path(sst.id);
        if let Err(e) = self.store.upload(&meta_path, meta).await {
            self.store
                .delete(&data_path)
                .await
                .map_err(HummockError::object_io_error)?;
            return Err(HummockError::object_io_error(e));
        }

        timer.observe_duration();

        if let CachePolicy::Fill = policy {
            // TODO: use concurrent put object
            for (block_idx, meta) in sst.meta.block_metas.iter().enumerate() {
                let offset = meta.offset as usize;
                let len = meta.len as usize;
                let block = Arc::new(Block::decode(data.slice(offset..offset + len))?);
                self.block_cache
                    .insert(sst.id, block_idx as u64, block)
                    .await
            }
        }

        Ok(len)
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: u64,
        policy: CachePolicy,
    ) -> HummockResult<Arc<Block>> {
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
                .store
                .read(&data_path, Some(block_loc))
                .await
                .map_err(HummockError::object_io_error)?;
            let block = Block::decode(block_data)?;

            timer.observe_duration();
            Ok(Arc::new(block))
        };

        match policy {
            CachePolicy::Fill => {
                self.block_cache
                    .get_or_insert_with(sst.id, block_index, fetch_block)
                    .await
            }
            CachePolicy::NotFill => match self.block_cache.get(sst.id, block_index) {
                Some(block) => Ok(block),
                None => fetch_block.await,
            },
            CachePolicy::Disable => fetch_block.await,
        }
    }

    pub async fn sstable(&self, sst_id: u64) -> HummockResult<Arc<Sstable>> {
        let fetch = async move {
            let path = self.get_sst_meta_path(sst_id);
            let buf = self
                .store
                .read(&path, None)
                .await
                .map_err(HummockError::object_io_error)?;
            let meta = SstableMeta::decode(&mut &buf[..])?;
            let sst = Arc::new(Sstable { id: sst_id, meta });
            Ok::<_, HummockError>(sst)
        };

        self.meta_cache
            .try_get_with(sst_id, fetch)
            .await
            .map_err(HummockError::other)
    }

    pub fn get_sst_meta_path(&self, sst_id: u64) -> String {
        format!("{}/{}.meta", self.path, sst_id)
    }

    pub fn get_sst_data_path(&self, sst_id: u64) -> String {
        format!("{}/{}.data", self.path, sst_id)
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
