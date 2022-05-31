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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use fail::fail_point;
use futures::channel::oneshot::{channel, Sender};
use futures::future::{try_join_all, FutureExt};
use risingwave_common::object::{get_local_path, BlockLocation, ObjectStoreRef};
use risingwave_hummock_sdk::{is_remote_sst_id, HummockSSTableId};

use super::{Block, BlockCache, Sstable, SstableMeta};
use crate::hummock::{BlockHolder, CachableEntry, HummockError, HummockResult, LruCache};
use crate::monitor::StoreLocalStatistic;

const MAX_META_CACHE_SHARD_BITS: usize = 5;
const MIN_BUFFER_SIZE_PER_SHARD: usize = 64 * 1024 * 1024; // 64MB
const PREFETCH_BLOCK_COUNT: usize = 20;

pub type TableHolder = CachableEntry<HummockSSTableId, Box<Sstable>>;

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill,
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Arc<LruCache<HummockSSTableId, Box<Sstable>>>,
    prefetch_request: Arc<Mutex<HashMap<u64, Vec<Sender<()>>>>>,
}

impl SstableStore {
    pub fn new(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
    ) -> Self {
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        let meta_cache = Arc::new(LruCache::new(shard_bits, meta_cache_capacity));
        Self {
            path,
            store,
            block_cache: BlockCache::new(block_cache_capacity),
            meta_cache,
            prefetch_request: Arc::new(Default::default()),
        }
    }

    pub async fn put(&self, sst: Sstable, data: Bytes, policy: CachePolicy) -> HummockResult<()> {
        self.put_sst_data(sst.id, data.clone()).await?;

        fail_point!("metadata_upload_err");
        if let Err(e) = self.put_meta(&sst).await {
            self.delete_sst_data(sst.id).await?;
            return Err(e);
        }

        if let CachePolicy::Fill = policy {
            for (block_idx, meta) in sst.meta.block_metas.iter().enumerate() {
                let offset = meta.offset as usize;
                let len = meta.len as usize;
                self.add_block_cache(sst.id, block_idx as u64, data.slice(offset..offset + len))
                    .unwrap();
            }
            self.meta_cache
                .insert(sst.id, sst.id, sst.encoded_size(), Box::new(sst.clone()));
        }

        Ok(())
    }

    pub async fn get_with_prefetch(
        &self,
        sst: &Sstable,
        block_index: u64,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        loop {
            stats.cache_data_block_total += 1;
            if let Some(block) = self.block_cache.get(sst.id, block_index) {
                return Ok(block);
            }
            stats.cache_data_block_miss += 1;
            let timer = Instant::now();
            let pending_request = {
                let mut pending_request = self.prefetch_request.lock().unwrap();
                if let Some(que) = pending_request.get_mut(&sst.id) {
                    let (tx, rc) = channel();
                    que.push(tx);
                    Some(rc)
                } else {
                    // query again to avoid the previous prefetch request just finished before we
                    // get lock.
                    if let Some(block) = self.block_cache.get(sst.id, block_index) {
                        return Ok(block);
                    }
                    pending_request.insert(sst.id, vec![]);
                    None
                }
            };
            if let Some(rc) = pending_request {
                let _ = rc.await;
                continue;
            }
            let ret = self.get_data(sst, block_index as usize).await;
            let mut prefetch_request = self.prefetch_request.lock().unwrap();
            let pending_requests = prefetch_request.remove(&sst.id).unwrap();
            for p in pending_requests {
                let _ = p.send(());
            }
            stats.remote_io_time += timer.elapsed().as_secs_f64();
            return ret;
        }
    }

    async fn put_meta(&self, sst: &Sstable) -> HummockResult<()> {
        let meta_path = self.get_sst_meta_path(sst.id);
        let meta = Bytes::from(sst.meta.encode_to_bytes());
        self.store
            .upload(&meta_path, meta)
            .await
            .map_err(HummockError::object_io_error)
    }

    async fn put_sst_data(&self, sst_id: HummockSSTableId, data: Bytes) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(sst_id);
        self.store
            .upload(&data_path, data)
            .await
            .map_err(HummockError::object_io_error)
    }

    async fn delete_sst_data(&self, sst_id: HummockSSTableId) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(sst_id);
        self.store
            .delete(&data_path)
            .await
            .map_err(HummockError::object_io_error)
    }

    pub fn add_block_cache(
        &self,
        sst_id: HummockSSTableId,
        block_idx: u64,
        block_data: Bytes,
    ) -> HummockResult<()> {
        let block = Box::new(Block::decode(block_data)?);
        self.block_cache.insert(sst_id, block_idx, block);
        Ok(())
    }

    pub async fn get_data(&self, sst: &Sstable, block_index: usize) -> HummockResult<BlockHolder> {
        let block_meta = sst
            .meta
            .block_metas
            .get(block_index)
            .ok_or_else(HummockError::invalid_block)?;
        let mut read_size = block_meta.len;
        let end_index = std::cmp::min(
            block_index + 1 + PREFETCH_BLOCK_COUNT,
            sst.meta.block_metas.len(),
        );
        if block_index + 1 < sst.meta.block_metas.len() {
            for block in &sst.meta.block_metas[(block_index + 1)..end_index] {
                read_size += block.len;
            }
        }

        let block_loc = BlockLocation {
            offset: block_meta.offset as usize,
            size: read_size as usize,
        };
        let data_path = self.get_sst_data_path(sst.id);
        let block_data = self
            .store
            .read(&data_path, Some(block_loc))
            .await
            .map_err(HummockError::object_io_error)?;
        let block = Block::decode(block_data.slice(..block_meta.len as usize))?;
        let ret = self
            .block_cache
            .insert(sst.id, block_index as u64, Box::new(block));
        if block_index + 1 < sst.meta.block_metas.len() {
            let mut index_offset = block_index as u64 + 1;
            let mut offset = block_meta.len as usize;
            for block_meta in &sst.meta.block_metas[(block_index + 1)..end_index] {
                let end_offset = offset + block_meta.len as usize;
                let block = Block::decode(block_data.slice(offset..end_offset))?;
                self.block_cache
                    .insert(sst.id, index_offset, Box::new(block));
                offset = end_offset;
                index_offset += 1;
            }
        }
        Ok(ret)
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: u64,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        stats.cache_data_block_total += 1;
        let fetch_block = async {
            stats.cache_data_block_miss += 1;
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
            Ok(Box::new(block))
        };

        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };

        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };

        match policy {
            CachePolicy::Fill => {
                self.block_cache
                    .get_or_insert_with(sst.id, block_index, fetch_block)
                    .await
            }
            CachePolicy::NotFill => match self.block_cache.get(sst.id, block_index) {
                Some(block) => Ok(block),
                None => fetch_block.await.map(BlockHolder::from_owned_block),
            },
            CachePolicy::Disable => fetch_block.await.map(BlockHolder::from_owned_block),
        }
    }

    pub async fn prefetch_sstables(&self, sst_ids: Vec<u64>) -> HummockResult<()> {
        let mut results = vec![];
        for sst_id in sst_ids {
            let id = sst_id;
            let f = self
                .meta_cache
                .lookup_with_request_dedup(sst_id, sst_id, move || async move {
                    let path = self.get_sst_meta_path(id);
                    let buf = self
                        .store
                        .read(&path, None)
                        .await
                        .map_err(HummockError::object_io_error)?;
                    let size = buf.len();
                    let meta = SstableMeta::decode(&mut &buf[..])?;
                    let sst = Box::new(Sstable { id, meta });
                    Ok((sst, size))
                })
                .map(|result| match result {
                    Ok(inner_result) => inner_result,
                    Err(e) => Err(HummockError::other(format!(
                        "prefetch table lookup request dedup get cancel: {:?}",
                        e,
                    ))),
                });
            results.push(f);
        }
        let _ = try_join_all(results).await?;
        Ok(())
    }

    pub async fn sstable(
        &self,
        sst_id: HummockSSTableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        stats.cache_meta_block_total += 1;

        let entry = self
            .meta_cache
            .lookup_with_request_dedup::<_, HummockError, _>(sst_id, sst_id, || async {
                stats.cache_meta_block_miss += 1;
                let path = self.get_sst_meta_path(sst_id);
                let buf = self
                    .store
                    .read(&path, None)
                    .await
                    .map_err(HummockError::object_io_error)?;
                let size = buf.len();
                let meta = SstableMeta::decode(&mut &buf[..])?;
                let sst = Box::new(Sstable { id: sst_id, meta });
                Ok((sst, size))
            })
            .await
            .map_err(|e| {
                HummockError::other(format!(
                    "meta cache lookup request dedup get cancel: {:?}",
                    e,
                ))
            })??;
        Ok(entry)
    }

    pub fn get_sst_meta_path(&self, sst_id: HummockSSTableId) -> String {
        let mut ret = format!("{}/{}.meta", self.path, sst_id);
        if !is_remote_sst_id(sst_id) {
            ret = get_local_path(&ret);
        }
        ret
    }

    pub fn get_sst_data_path(&self, sst_id: HummockSSTableId) -> String {
        let mut ret = format!("{}/{}.data", self.path, sst_id);
        if !is_remote_sst_id(sst_id) {
            ret = get_local_path(&ret);
        }
        ret
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }

    pub fn get_meta_cache(&self) -> Arc<LruCache<HummockSSTableId, Box<Sstable>>> {
        self.meta_cache.clone()
    }

    pub fn get_block_cache(&self) -> BlockCache {
        self.block_cache.clone()
    }

    #[cfg(test)]
    pub fn clear_block_cache(&self) {
        self.block_cache.clear();
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
