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

use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use foyer::common::code::{Key, Value};
use foyer::intrusive::eviction::lfu::LfuConfig;
use foyer::storage::admission::rated_ticket::RatedTicketAdmissionPolicy;
use foyer::storage::admission::AdmissionPolicy;
use foyer::storage::compress::Compression;
use foyer::storage::device::fs::FsDeviceConfig;
pub use foyer::storage::metrics::set_metrics_registry as set_foyer_metrics_registry;
use foyer::storage::reinsertion::ReinsertionPolicy;
use foyer::storage::runtime::{
    RuntimeConfig, RuntimeLazyStore, RuntimeLazyStoreConfig, RuntimeLazyStoreWriter,
};
use foyer::storage::storage::{Storage, StorageWriter};
use foyer::storage::store::{LfuFsStoreConfig, NoneStore, NoneStoreWriter};
use risingwave_hummock_sdk::HummockSstableObjectId;

use crate::hummock::{Block, Sstable, SstableMeta};

pub mod preclude {
    pub use foyer::storage::storage::{
        AsyncStorageExt, ForceStorageExt, Storage, StorageExt, StorageWriter,
    };
}

pub type Result<T> = core::result::Result<T, FileCacheError>;

pub type EvictionConfig = foyer::intrusive::eviction::lfu::LfuConfig;
pub type DeviceConfig = foyer::storage::device::fs::FsDeviceConfig;

pub type FileCacheResult<T> = foyer::storage::error::Result<T>;
pub type FileCacheError = foyer::storage::error::Error;

#[derive(Debug)]
pub struct FileCacheConfig<K, V>
where
    K: Key,
    V: Value,
{
    pub name: String,
    pub dir: PathBuf,
    pub capacity: usize,
    pub file_capacity: usize,
    pub device_align: usize,
    pub device_io_size: usize,
    pub flushers: usize,
    pub reclaimers: usize,
    pub reclaim_rate_limit: usize,
    pub recover_concurrency: usize,
    pub lfu_window_to_cache_size_ratio: usize,
    pub lfu_tiny_lru_capacity_ratio: f64,
    pub insert_rate_limit: usize,
    pub ring_buffer_capacity: usize,
    pub catalog_bits: usize,
    pub admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>>,
    pub reinsertions: Vec<Arc<dyn ReinsertionPolicy<Key = K, Value = V>>>,
    pub compression: Compression,
}

impl<K, V> Clone for FileCacheConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            dir: self.dir.clone(),
            capacity: self.capacity,
            file_capacity: self.file_capacity,
            device_align: self.device_align,
            device_io_size: self.device_io_size,
            flushers: self.flushers,
            reclaimers: self.reclaimers,
            reclaim_rate_limit: self.reclaim_rate_limit,
            recover_concurrency: self.recover_concurrency,
            lfu_window_to_cache_size_ratio: self.lfu_window_to_cache_size_ratio,
            lfu_tiny_lru_capacity_ratio: self.lfu_tiny_lru_capacity_ratio,
            insert_rate_limit: self.insert_rate_limit,
            ring_buffer_capacity: self.ring_buffer_capacity,
            catalog_bits: self.catalog_bits,
            admissions: self.admissions.clone(),
            reinsertions: self.reinsertions.clone(),
            compression: self.compression,
        }
    }
}

#[derive(Debug)]
pub enum FileCacheWriter<K, V>
where
    K: Key,
    V: Value,
{
    Foyer {
        writer: RuntimeLazyStoreWriter<K, V>,
    },
    None {
        writer: NoneStoreWriter<K, V>,
    },
}

impl<K, V> From<RuntimeLazyStoreWriter<K, V>> for FileCacheWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: RuntimeLazyStoreWriter<K, V>) -> Self {
        Self::Foyer { writer }
    }
}

impl<K, V> From<NoneStoreWriter<K, V>> for FileCacheWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: NoneStoreWriter<K, V>) -> Self {
        Self::None { writer }
    }
}

impl<K, V> StorageWriter for FileCacheWriter<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        match self {
            FileCacheWriter::Foyer { writer } => writer.key(),
            FileCacheWriter::None { writer } => writer.key(),
        }
    }

    fn weight(&self) -> usize {
        match self {
            FileCacheWriter::Foyer { writer } => writer.weight(),
            FileCacheWriter::None { writer } => writer.weight(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            FileCacheWriter::Foyer { writer } => writer.judge(),
            FileCacheWriter::None { writer } => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            FileCacheWriter::Foyer { writer } => writer.force(),
            FileCacheWriter::None { writer } => writer.force(),
        }
    }

    async fn finish(self, value: Self::Value) -> FileCacheResult<bool> {
        match self {
            FileCacheWriter::Foyer { writer } => writer.finish(value).await,
            FileCacheWriter::None { writer } => writer.finish(value).await,
        }
    }
}

#[derive(Debug)]
pub enum FileCache<K, V>
where
    K: Key,
    V: Value,
{
    Foyer { store: RuntimeLazyStore<K, V> },
    None { store: NoneStore<K, V> },
}

impl<K, V> Clone for FileCache<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        match self {
            Self::Foyer { store } => Self::Foyer {
                store: store.clone(),
            },
            Self::None { store } => Self::None {
                store: store.clone(),
            },
        }
    }
}

impl<K, V> FileCache<K, V>
where
    K: Key,
    V: Value,
{
    pub fn none() -> Self {
        Self::None {
            store: NoneStore::default(),
        }
    }
}

impl<K, V> Storage for FileCache<K, V>
where
    K: Key,
    V: Value,
{
    type Config = FileCacheConfig<K, V>;
    type Key = K;
    type Value = V;
    type Writer = FileCacheWriter<K, V>;

    async fn open(config: Self::Config) -> FileCacheResult<Self> {
        let mut admissions = config.admissions;
        if config.insert_rate_limit > 0 {
            admissions.push(Arc::new(RatedTicketAdmissionPolicy::new(
                config.insert_rate_limit,
            )));
        }

        let c = RuntimeLazyStoreConfig {
            store: LfuFsStoreConfig {
                name: config.name.clone(),
                eviction_config: LfuConfig {
                    window_to_cache_size_ratio: config.lfu_window_to_cache_size_ratio,
                    tiny_lru_capacity_ratio: config.lfu_tiny_lru_capacity_ratio,
                },
                device_config: FsDeviceConfig {
                    dir: config.dir,
                    capacity: config.capacity,
                    file_capacity: config.file_capacity,
                    align: config.device_align,
                    io_size: config.device_io_size,
                },
                ring_buffer_capacity: config.ring_buffer_capacity,
                catalog_bits: config.catalog_bits,
                admissions,
                reinsertions: config.reinsertions,
                flusher_buffer_size: 131072, // TODO: make it configurable
                flushers: config.flushers,
                reclaimers: config.reclaimers,
                reclaim_rate_limit: config.reclaim_rate_limit,
                clean_region_threshold: config.reclaimers + config.reclaimers / 2,
                recover_concurrency: config.recover_concurrency,
                compression: config.compression,
            }
            .into(),
            runtime: RuntimeConfig {
                worker_threads: None,
                thread_name: Some(config.name),
            },
        };
        let store = RuntimeLazyStore::open(c).await?;
        Ok(Self::Foyer { store })
    }

    fn is_ready(&self) -> bool {
        match self {
            FileCache::Foyer { store } => store.is_ready(),
            FileCache::None { store } => store.is_ready(),
        }
    }

    async fn close(&self) -> FileCacheResult<()> {
        match self {
            FileCache::Foyer { store } => store.close().await,
            FileCache::None { store } => store.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        match self {
            FileCache::Foyer { store } => store.writer(key, weight).into(),
            FileCache::None { store } => store.writer(key, weight).into(),
        }
    }

    fn exists(&self, key: &Self::Key) -> FileCacheResult<bool> {
        match self {
            FileCache::Foyer { store } => store.exists(key),
            FileCache::None { store } => store.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> FileCacheResult<Option<Self::Value>> {
        match self {
            FileCache::Foyer { store } => store.lookup(key).await,
            FileCache::None { store } => store.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> FileCacheResult<bool> {
        match self {
            FileCache::Foyer { store } => store.remove(key),
            FileCache::None { store } => store.remove(key),
        }
    }

    fn clear(&self) -> FileCacheResult<()> {
        match self {
            FileCache::Foyer { store } => store.clear(),
            FileCache::None { store } => store.clear(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_idx: u64,
}

impl Key for SstableBlockIndex {
    fn serialized_len(&self) -> usize {
        8 + 8 // sst_id (8B) + block_idx (8B)
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

impl Value for Box<Sstable> {
    fn serialized_len(&self) -> usize {
        8 + self.meta.encoded_size() // id (8B) + meta size
    }

    fn write(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.id);
        // TODO(MrCroxx): avoid buffer copy
        let mut buffer = vec![];
        self.meta.encode_to(&mut buffer);
        buf.put_slice(&buffer[..])
    }

    fn read(mut buf: &[u8]) -> Self {
        let id = buf.get_u64();
        let meta = SstableMeta::decode(buf).unwrap();
        Box::new(Sstable::new(id, meta))
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
