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

use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes};
use foyer::common::code::{Key, Value};
use foyer::storage::admission::rated_random::RatedRandom;
use foyer::storage::admission::AdmissionPolicy;
use foyer::storage::LfuFsStoreConfig;
use prometheus::Registry;
use risingwave_hummock_sdk::HummockSstableObjectId;

use super::Block;

#[derive(thiserror::Error, Debug)]
pub enum FileCacheError {
    #[error("foyer error: {0}")]
    Foyer(#[from] foyer::storage::error::Error),
}

impl FileCacheError {
    fn foyer(e: foyer::storage::error::Error) -> Self {
        Self::Foyer(e)
    }
}

pub type Result<T> = core::result::Result<T, FileCacheError>;

pub type EvictionConfig = foyer::intrusive::eviction::lfu::LfuConfig;
pub type DeviceConfig = foyer::storage::device::fs::FsDeviceConfig;

pub type FoyerStore = foyer::storage::LfuFsStore<SstableBlockIndex, Box<Block>>;

pub struct FoyerStoreConfig {
    pub dir: String,
    pub capacity: usize,
    pub file_capacity: usize,
    pub buffer_pool_size: usize,
    pub device_align: usize,
    pub device_io_size: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub recover_concurrency: usize,
    pub lfu_window_to_cache_size_ratio: usize,
    pub lfu_tiny_lru_capacity_ratio: f64,
    pub rated_random_rate: usize,
    pub prometheus_registry: Option<Registry>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_idx: u64,
}

impl Key for SstableBlockIndex {
    fn serialized_len(&self) -> usize {
        8 + 8
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.sst_id);
        buf.put_u64(self.block_idx);
    }

    fn read(mut buf: &[u8]) -> Self {
        let sst_id = buf.get_u64();
        let block_idx = buf.get_u64();
        Self { sst_id, block_idx }
    }
}

impl Value for Box<Block> {
    fn serialized_len(&self) -> usize {
        self.raw_data().len()
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_slice(self.raw_data())
    }

    fn read(buf: &[u8]) -> Self {
        let data = Bytes::copy_from_slice(buf);
        let block = Block::decode_from_raw(data);
        Box::new(block)
    }
}

#[derive(Clone)]
pub enum FileCache {
    None,
    Foyer(Arc<FoyerStore>),
}

impl FileCache {
    pub fn none() -> Self {
        Self::None
    }

    pub async fn foyer(config: FoyerStoreConfig) -> Result<Self> {
        let file_capacity = config.file_capacity;
        let capacity = config.capacity;
        let capacity = capacity - (capacity % file_capacity);

        let mut admissions: Vec<
            Arc<dyn AdmissionPolicy<Key = SstableBlockIndex, Value = Box<Block>>>,
        > = vec![];
        if config.rated_random_rate > 0 {
            let rr = RatedRandom::new(
                config.rated_random_rate * 1024 * 1024,
                Duration::from_millis(100),
            );
            admissions.push(Arc::new(rr));
        }

        let c = LfuFsStoreConfig {
            eviction_config: EvictionConfig {
                window_to_cache_size_ratio: config.lfu_window_to_cache_size_ratio,
                tiny_lru_capacity_ratio: config.lfu_tiny_lru_capacity_ratio,
            },
            device_config: DeviceConfig {
                dir: PathBuf::from(config.dir.clone()),
                capacity,
                file_capacity,
                align: config.device_align,
                io_size: config.device_io_size,
            },
            admissions,
            reinsertions: vec![],
            buffer_pool_size: config.buffer_pool_size,
            flushers: config.flushers,
            reclaimers: config.reclaimers,
            recover_concurrency: config.recover_concurrency,
            prometheus_registry: config.prometheus_registry,
        };
        let store = FoyerStore::open(c).await.map_err(FileCacheError::foyer)?;
        Ok(Self::Foyer(store))
    }

    pub async fn insert(&self, key: SstableBlockIndex, value: Box<Block>) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => {
                let len = value.serialized_len();

                let time = std::time::Instant::now();
                let res = store
                    .insert(key.clone(), value)
                    .await
                    .map(|_| ())
                    .map_err(FileCacheError::foyer);
                let elapsed = time.elapsed();

                if elapsed.as_millis() > 100 {
                    tracing::warn!(
                        "slow file cache insertion, key: {:?}, value len: {:?}",
                        key,
                        len
                    );
                }
                res
            }
        }
    }

    pub fn remove(&self, key: &SstableBlockIndex) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => {
                store.remove(key);
                Ok(())
            }
        }
    }

    pub async fn lookup(&self, key: &SstableBlockIndex) -> Result<Option<Box<Block>>> {
        match self {
            FileCache::None => Ok(None),
            FileCache::Foyer(store) => store.lookup(key).await.map_err(FileCacheError::foyer),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::FullKey;

    use super::*;
    use crate::hummock::{
        BlockBuilder, BlockBuilderOptions, BlockHolder, BlockIterator, CompressionAlgorithm,
    };

    #[test]
    fn test_enc_dec() {
        let options = BlockBuilderOptions {
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };

        let mut builder = BlockBuilder::new(options);
        builder.add_for_test(construct_full_key_struct(0, b"k1", 1), b"v01");
        builder.add_for_test(construct_full_key_struct(0, b"k2", 2), b"v02");
        builder.add_for_test(construct_full_key_struct(0, b"k3", 3), b"v03");
        builder.add_for_test(construct_full_key_struct(0, b"k4", 4), b"v04");

        let block = Box::new(
            Block::decode(
                builder.build().to_vec().into(),
                builder.uncompressed_block_size(),
            )
            .unwrap(),
        );

        let mut buf = vec![0; block.serialized_len()];
        block.write(&mut buf[..]);

        let block = <Box<Block> as Value>::read(&buf[..]);

        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k1", 1), bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k2", 2), bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k3", 3), bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k4", 4), bi.key());
        assert_eq!(b"v04", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    pub fn construct_full_key_struct(
        table_id: u32,
        table_key: &[u8],
        epoch: u64,
    ) -> FullKey<&[u8]> {
        FullKey::for_test(TableId::new(table_id), table_key, epoch)
    }
}
