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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use futures::Future;
use risingwave_common::cache::{CachableEntry, LruCache};
use risingwave_hummock_sdk::HummockSSTableId;

use super::{Block, HummockResult};
use crate::hummock::HummockError;

const MIN_BUFFER_SIZE_PER_SHARD: usize = 32 * 1024 * 1024;

enum BlockEntry {
    Cache(CachableEntry<(HummockSSTableId, u64), Box<Block>>),
    Owned(Box<Block>),
    RefEntry(Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    block: *const Block,
}

impl BlockHolder {
    pub fn from_ref_block(block: Arc<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::RefEntry(block),
            block: ptr,
        }
    }

    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_cached_block(entry: CachableEntry<(HummockSSTableId, u64), Box<Block>>) -> Self {
        let ptr = entry.value().as_ref() as *const _;
        Self {
            _handle: BlockEntry::Cache(entry),
            block: ptr,
        }
    }
}

impl Deref for BlockHolder {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<LruCache<(HummockSSTableId, u64), Box<Block>>>,
}

impl BlockCache {
    pub fn new(capacity: usize, mut max_shard_bits: usize) -> Self {
        if capacity == 0 {
            panic!("block cache capacity == 0");
        }
        while (capacity >> max_shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && max_shard_bits > 0 {
            max_shard_bits -= 1;
        }
        let cache = LruCache::new(max_shard_bits, capacity);
        Self {
            inner: Arc::new(cache),
        }
    }

    pub fn get(&self, sst_id: HummockSSTableId, block_idx: u64) -> Option<BlockHolder> {
        self.inner
            .lookup(Self::hash(sst_id, block_idx), &(sst_id, block_idx))
            .map(BlockHolder::from_cached_block)
    }

    pub fn insert(
        &self,
        sst_id: HummockSSTableId,
        block_idx: u64,
        block: Box<Block>,
    ) -> BlockHolder {
        BlockHolder::from_cached_block(self.inner.insert(
            (sst_id, block_idx),
            Self::hash(sst_id, block_idx),
            block.len(),
            block,
        ))
    }

    pub async fn get_or_insert_with<F>(
        &self,
        sst_id: HummockSSTableId,
        block_idx: u64,
        f: F,
    ) -> HummockResult<BlockHolder>
    where
        F: Future<Output = HummockResult<Box<Block>>>,
    {
        let h = Self::hash(sst_id, block_idx);
        let key = (sst_id, block_idx);
        let entry = self
            .inner
            .lookup_with_request_dedup::<_, HummockError, _>(h, key, || async {
                let block = f.await?;
                let len = block.len();
                Ok((block, len))
            })
            .await
            .map_err(|e| {
                HummockError::other(format!(
                    "block cache lookup request dedup get cancel: {:?}",
                    e,
                ))
            })??;
        Ok(BlockHolder::from_cached_block(entry))
    }

    fn hash(sst_id: HummockSSTableId, block_idx: u64) -> u64 {
        let mut hasher = DefaultHasher::default();
        sst_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        hasher.finish()
    }

    pub fn size(&self) -> usize {
        self.inner.get_memory_usage()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear(&self) {
        // This is only a method for test. Therefore it should be safe to call the unsafe method.
        unsafe {
            self.inner.clear();
        }
    }
}
