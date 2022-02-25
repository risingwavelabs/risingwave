use std::sync::Arc;

use bytes::BufMut;
use moka::future::Cache;

use super::Block;

pub type Key = Vec<u8>;

pub fn block_cache_key(sst_id: u64, block_id: u64) -> Key {
    let mut key = Vec::with_capacity(16);
    key.put_u64_le(sst_id);
    key.put_u64_le(block_id);
    key
}

pub struct BlockCache {
    inner: Cache<Vec<u8>, Arc<Block>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Cache::new(capacity as u64),
        }
    }

    pub fn get(&self, key: &Key) -> Option<Arc<Block>> {
        self.inner.get(key)
    }

    pub async fn insert(&self, key: Key, block: Arc<Block>) {
        self.inner.insert(key, block).await
    }
}

pub type BlockCacheRef = Arc<BlockCache>;
