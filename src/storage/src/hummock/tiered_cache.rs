// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use foyer::{
    Index, Metrics, ReadOnlyFileStoreConfig, TinyLfuConfig, TinyLfuReadOnlyFileStoreCache,
    TinyLfuReadOnlyFileStoreCacheConfig,
};
use risingwave_hummock_sdk::HummockSstableObjectId;

use super::{HummockError, HummockResult};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("foyer error: {0}")]
    FoyerError(#[from] foyer::Error),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Debug)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_index: usize,
}

impl Index for SstableBlockIndex {
    fn size() -> usize {
        16
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.sst_id);
        buf.put_u64(self.block_index as u64);
    }

    fn read(mut buf: &[u8]) -> Self {
        let sst_id = buf.get_u64();
        let block_index = buf.get_u64() as usize;
        Self {
            sst_id,
            block_index,
        }
    }
}

const FOYER_POOL_COUNT_BITS: usize = 5;
const FOYER_POOLS: usize = 1 << FOYER_POOL_COUNT_BITS;
const FOYER_TINYLFU_WINDOW_TO_CACHE_SIZE_RATIO: usize = 10;
const FOYER_TINYLFU_TINY_LRU_CAPACITY_RATIO: f64 = 0.01;
const FOYER_TRIGGER_RECLAIM_GARBAGE_RATIO: f64 = 0.5;
const FOYER_TRIGGER_RECLAIM_CAPACITY_RATION: f64 = 0.8;
const FOYER_TRIGGER_RANDOM_DROP_RATIO: f64 = 0.9;
const FOYER_RANDOM_DROP_RATIO: f64 = 0.2;

pub struct TieredCacheConfig {
    pub dir: String,
    pub capacity: usize,
    pub max_file_size: usize,
}

#[derive(Clone)]
pub enum TieredCache {
    Foyer(Arc<TinyLfuReadOnlyFileStoreCache<SstableBlockIndex, Vec<u8>>>),
    None,
}

impl TieredCache {
    pub async fn foyer(
        config: TieredCacheConfig,
        registry: prometheus::Registry,
    ) -> HummockResult<Self> {
        let policy_config = TinyLfuConfig {
            window_to_cache_size_ratio: FOYER_TINYLFU_WINDOW_TO_CACHE_SIZE_RATIO,
            tiny_lru_capacity_ratio: FOYER_TINYLFU_TINY_LRU_CAPACITY_RATIO,
        };

        let store_config = ReadOnlyFileStoreConfig {
            dir: PathBuf::from(config.dir),
            capacity: config.capacity / FOYER_POOLS,
            max_file_size: config.max_file_size,
            trigger_reclaim_garbage_ratio: FOYER_TRIGGER_RECLAIM_GARBAGE_RATIO,
            trigger_reclaim_capacity_ratio: FOYER_TRIGGER_RECLAIM_CAPACITY_RATION,
            trigger_random_drop_ratio: FOYER_TRIGGER_RANDOM_DROP_RATIO,
            random_drop_ratio: FOYER_RANDOM_DROP_RATIO,
        };

        let cache_config = TinyLfuReadOnlyFileStoreCacheConfig {
            capacity: config.capacity,
            pool_count_bits: FOYER_POOL_COUNT_BITS,
            policy_config,
            store_config,
        };
        let cache = TinyLfuReadOnlyFileStoreCache::open_with_registry(cache_config, registry)
            .await
            .map_err(HummockError::tiered_cache)?;
        Ok(Self::Foyer(Arc::new(cache)))
    }

    pub fn none() -> Self {
        Self::None
    }

    pub async fn insert(&self, index: SstableBlockIndex, data: Vec<u8>) -> HummockResult<()> {
        match self {
            Self::Foyer(cache) => cache
                .insert(index, data)
                .await
                .map(|_| ())
                .map_err(HummockError::tiered_cache),
            Self::None => Ok(()),
        }
    }

    pub async fn get(&self, index: &SstableBlockIndex) -> HummockResult<Option<Vec<u8>>> {
        match self {
            Self::Foyer(cache) => cache.get(index).await.map_err(HummockError::tiered_cache),
            Self::None => Ok(None),
        }
    }
}

pub type TieredCacheMetrics = Metrics;
