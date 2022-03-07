// TODO(MrCroxx): This file needs to be refactored.

use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use risingwave_pb::hummock::SstableMeta;

use super::{Block, BlockCache, Sstable};
use crate::hummock::{HummockError, HummockResult};
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
}

impl SstableStore {
    pub fn new(store: ObjectStoreRef, path: String) -> Self {
        Self {
            path,
            store,
            block_cache: BlockCache::new(65536),
        }
    }

    pub async fn put(&self, sst: &Sstable, data: Bytes, policy: CachePolicy) -> HummockResult<()> {
        // TODO(MrCroxx): Temporarily disable meta checksum. Make meta a normal block later and
        // reuse block encoding later.
        let meta = Bytes::from(sst.meta.encode_to_vec());

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

        if let CachePolicy::Fill = policy {
            // TODO: use concurrent put object
            for (block_idx, meta) in sst.meta.block_metas.iter().enumerate() {
                let offset = meta.offset as usize;
                let len = meta.len as usize;
                let block = Block::decode(data.slice(offset..offset + len), offset)?;
                self.block_cache
                    .insert(sst.id, block_idx as u64, block)
                    .await
            }
        }

        Ok(())
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: u64,
        policy: CachePolicy,
    ) -> HummockResult<Arc<Block>> {
        let fetch_block = async move {
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
            Block::decode(block_data, block_meta.offset as usize)
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
        let path = self.get_sst_meta_path(sst_id);
        let buf = self
            .store
            .read(&path, None)
            .await
            .map_err(HummockError::object_io_error)?;
        SstableMeta::decode(buf).map_err(HummockError::decode_error)
    }

    // TODO(MrCroxx): Maybe use `&SSTable` directly?
    fn get_sst_meta_path(&self, sst_id: u64) -> String {
        format!("{}/{}.meta", self.path, sst_id)
    }

    // TODO(MrCroxx): Maybe use `&SSTable` directly?
    fn get_sst_data_path(&self, sst_id: u64) -> String {
        format!("{}/{}.data", self.path, sst_id)
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
