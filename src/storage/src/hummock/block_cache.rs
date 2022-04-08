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

use bytes::{BufMut, Bytes, BytesMut};
use futures::Future;
use moka::future::Cache;

use super::{Block, HummockError, HummockResult, DEFAULT_ENTRY_SIZE};

pub struct BlockCache {
    inner: Cache<Bytes, Arc<Block>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let cache: Cache<Bytes, Arc<Block>> = Cache::builder()
            .weigher(|_k, v: &Arc<Block>| v.len() as u32)
            .initial_capacity(capacity / DEFAULT_ENTRY_SIZE)
            .max_capacity(capacity as u64)
            .build();
        Self { inner: cache }
    }

    // TODO: Optimize for concurrent get https://github.com/singularity-data/risingwave/pull/627#discussion_r817354730.
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
        self.inner
            .try_get_with(Self::key(sst_id, block_idx), f)
            .await
            .map_err(HummockError::other)
    }

    fn key(sst_id: u64, block_idx: u64) -> Bytes {
        let mut key = BytesMut::with_capacity(16);
        key.put_u64_le(sst_id);
        key.put_u64_le(block_idx);
        key.freeze()
    }
}
