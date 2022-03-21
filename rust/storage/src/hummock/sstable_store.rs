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
//
// TODO(MrCroxx): This file needs to be refactored.

use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;
use prost::Message;
use risingwave_pb::hummock::SstableMeta;

use super::{Block, BlockCache, Sstable};
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::StateStoreMetrics;
use crate::object::{BlockLocation, ObjectStoreRef};

// TODO: Define policy based on use cases (read / comapction / ...).
pub enum CachePolicy {
    Disable,
    Fill,
    NotFill,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    /// TODO: meta is also supposed to be block, and meta cache will be removed.
    meta_cache: Cache<u64, SstableMeta>,
    /// Statistics.
    stats: Option<Arc<StateStoreMetrics>>,
}

impl SstableStore {
    pub fn new(store: ObjectStoreRef, path: String, stats: Option<Arc<StateStoreMetrics>>) -> Self {
        Self {
            path,
            store,
            block_cache: BlockCache::new(65536),
            meta_cache: Cache::new(1024),
            stats,
        }
    }

    pub async fn put(
        &self,
        sst: &Sstable,
        data: Bytes,
        policy: CachePolicy,
    ) -> HummockResult<usize> {
        let timer = if self.stats.is_some() {
            Some(
                self.stats
                    .as_ref()
                    .unwrap()
                    .sst_block_put_remote_duration
                    .start_timer(),
            )
        } else {
            None
        };

        // TODO(MrCroxx): Temporarily disable meta checksum. Make meta a normal block later and
        // reuse block encoding later.
        let meta = Bytes::from(sst.meta.encode_to_vec());
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

        if let Some(timer) = timer {
            timer.observe_duration();
        }

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
        if let Some(stats) = &self.stats {
            stats.sst_block_request_counts.inc();
        }

        let fetch_block = async move {
            let timer = if self.stats.is_some() {
                self.stats
                    .as_ref()
                    .unwrap()
                    .sst_block_request_miss_counts
                    .inc();
                Some(
                    self.stats
                        .as_ref()
                        .unwrap()
                        .sst_block_fetch_remote_duration
                        .start_timer(),
                )
            } else {
                None
            };

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

            if let Some(timer) = timer {
                timer.observe_duration();
            }
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

    pub async fn meta(&self, sst_id: u64) -> HummockResult<SstableMeta> {
        // TODO(MrCroxx): meta should also be a `Block` later and managed in block cache.
        if let Some(meta) = self.meta_cache.get(&sst_id) {
            return Ok(meta);
        }
        let path = self.get_sst_meta_path(sst_id);
        let buf = self
            .store
            .read(&path, None)
            .await
            .map_err(HummockError::object_io_error)?;
        let meta = SstableMeta::decode(buf).map_err(HummockError::decode_error)?;
        self.meta_cache.insert(sst_id, meta.clone()).await;
        Ok(meta)
    }

    // TODO(MrCroxx): Maybe use `&SSTable` directly?
    pub fn get_sst_meta_path(&self, sst_id: u64) -> String {
        format!("{}/{}.meta", self.path, sst_id)
    }

    // TODO(MrCroxx): Maybe use `&SSTable` directly?
    pub fn get_sst_data_path(&self, sst_id: u64) -> String {
        format!("{}/{}.data", self.path, sst_id)
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
