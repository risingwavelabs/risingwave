// TODO(MrCroxx): This file needs to be refactored.

use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;
use prost::Message;
use risingwave_pb::hummock::SstableMeta;

use super::Block;
use crate::hummock::{HummockError, HummockResult};
use crate::object::{BlockLocation, ObjectStoreRef};

pub struct SstableManager {
    path: String,
    store: ObjectStoreRef,
    // TODO: Refactor block cache.
    block_cache: Cache<(u64, u64), Arc<Block>>,
}

impl SstableManager {
    pub fn new(store: ObjectStoreRef, path: String) -> Self {
        Self {
            path,
            store,
            block_cache: Cache::new(65536),
        }
    }

    // TODO(MrCroxx): Maybe use `&SSTable` and `data` directly?
    // TODO(MrCroxx): Add arg to decide if to fill block cache.
    pub async fn put(&self, sst_id: u64, meta: &SstableMeta, data: Bytes) -> HummockResult<()> {
        // TODO(MrCroxx): Temporarily disable meta checksum. Make meta a normal block later and
        // reuse block encoding later.
        let meta = Bytes::from(meta.encode_to_vec());

        let data_path = self.get_sst_data_path(sst_id);
        self.store
            .upload(&data_path, data)
            .await
            .map_err(HummockError::object_io_error)?;

        let meta_path = self.get_sst_meta_path(sst_id);
        if let Err(e) = self.store.upload(&meta_path, meta).await {
            self.store
                .delete(&data_path)
                .await
                .map_err(HummockError::object_io_error)?;
            return Err(HummockError::object_io_error(e));
        }
        Ok(())
    }

    // TODO(MrCroxx): Maybe use `&SSTable` directly?
    pub async fn get(
        &self,
        sst_id: u64,
        meta: &SstableMeta,
        block_index: u64,
    ) -> HummockResult<Arc<Block>> {
        let data_path = self.get_sst_data_path(sst_id);

        // TODO(MrCroxx): Support concurrent get!
        match self.block_cache.get(&(sst_id, block_index)) {
            Some(block) => Ok(block),
            None => {
                let block_meta = meta
                    .block_metas
                    .get(block_index as usize)
                    .ok_or_else(HummockError::invalid_block)?;
                let block_loc = BlockLocation {
                    offset: block_meta.offset as usize,
                    size: block_meta.len as usize,
                };
                let block_data = self
                    .store
                    .read(&data_path, Some(block_loc))
                    .await
                    .map_err(HummockError::object_io_error)?;
                let block = Block::decode(block_data, 0)?;
                self.block_cache
                    .insert((sst_id, block_index), block.clone())
                    .await;
                Ok(block)
            }
        }
    }

    pub async fn getv(
        &self,
        sst_id: u64,
        meta: &SstableMeta,
        block_indices: Vec<u64>,
    ) -> HummockResult<Vec<Arc<Block>>> {
        let data_path = self.get_sst_data_path(sst_id);

        let mut blocks = Vec::with_capacity(block_indices.len());

        // TODO(MrCroxx): Support concurrent get!
        for index in block_indices {
            let block = match self.block_cache.get(&(sst_id, index)) {
                Some(block) => block,
                None => {
                    let block_meta = meta.block_metas.get(index as usize).unwrap();
                    let block_loc = BlockLocation {
                        offset: block_meta.offset as usize,
                        size: block_meta.len as usize,
                    };
                    let block_data = self
                        .store
                        .read(&data_path, Some(block_loc))
                        .await
                        .map_err(HummockError::object_io_error)?;
                    let block = Block::decode(block_data, 0)?;
                    self.block_cache
                        .insert((sst_id, index), block.clone())
                        .await;
                    block
                }
            };
            blocks.push(block);
        }
        Ok(blocks)
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

pub type SSTableManagerRef = Arc<SstableManager>;
