use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::Future;
use moka::future::Cache;

use super::{Block, HummockError, HummockResult};

pub struct BlockCache {
    inner: Cache<Bytes, Arc<Block>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Cache::new(capacity as u64),
        }
    }

    // TODO: Optimize for concurrent get https://github.com/singularity-data/risingwave-dev/pull/627#discussion_r817354730 .
    pub fn get(&self, sst_id: u64, block_idx: u64) -> Option<Arc<Block>> {
        self.inner.get(&Self::key(sst_id, block_idx))
    }

    pub async fn insert(&self, sst_id: u64, block_idx: u64, block: Arc<Block>) {
        self.inner.insert(Self::key(sst_id, block_idx), block).await
    }

    pub async fn get_or_insert_with<F>(
        &self,
        sst_id: u64,
        block_idx: u64,
        f: F,
    ) -> HummockResult<Arc<Block>>
    where
        F: Future<Output = HummockResult<Arc<Block>>>,
    {
        match self
            .inner
            .get_or_try_insert_with(Self::key(sst_id, block_idx), f)
            .await
        {
            Ok(block) => Ok(block),
            Err(arc_e) => {
                Err(Arc::try_unwrap(arc_e).map_err(|e| HummockError::Other(e.to_string()))?)
            }
        }
    }

    fn key(sst_id: u64, block_idx: u64) -> Bytes {
        let mut key = BytesMut::with_capacity(16);
        key.put_u64_le(sst_id);
        key.put_u64_le(block_idx);
        key.freeze()
    }
}
