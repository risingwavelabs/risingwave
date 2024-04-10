// Copyright 2024 RisingWave Labs
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
use foyer::common::code::{CodingResult, Cursor, Key, Value};
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

use crate::hummock::{Block, HummockResult, Sstable, SstableMeta};

pub mod preclude {
    pub use foyer::storage::storage::{
        AsyncStorageExt, ForceStorageExt, Storage, StorageExt, StorageWriter,
    };
}

pub type Result<T> = core::result::Result<T, FileCacheError>;

pub type FileCacheEvictionConfig = foyer::intrusive::eviction::lfu::LfuConfig;
pub type DeviceConfig = foyer::storage::device::fs::FsDeviceConfig;

pub type FileCacheResult<T> = foyer::storage::error::Result<T>;
pub type FileCacheError = foyer::storage::error::Error;
pub type FileCacheCompression = foyer::storage::compress::Compression;

fn copy(src: impl AsRef<[u8]>, mut dst: impl BufMut + AsRef<[u8]>) -> usize {
    let n = std::cmp::min(src.as_ref().len(), dst.as_ref().len());
    dst.put_slice(&src.as_ref()[..n]);
    n
}

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
    pub recover_concurrency: usize,
    pub lfu_window_to_cache_size_ratio: usize,
    pub lfu_tiny_lru_capacity_ratio: f64,
    pub insert_rate_limit: usize,
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
            recover_concurrency: self.recover_concurrency,
            lfu_window_to_cache_size_ratio: self.lfu_window_to_cache_size_ratio,
            lfu_tiny_lru_capacity_ratio: self.lfu_tiny_lru_capacity_ratio,
            insert_rate_limit: self.insert_rate_limit,
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

    fn compression(&self) -> Compression {
        match self {
            FileCacheWriter::Foyer { writer } => writer.compression(),
            FileCacheWriter::None { writer } => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            FileCacheWriter::Foyer { writer } => writer.set_compression(compression),
            FileCacheWriter::None { writer } => writer.set_compression(compression),
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

    pub fn is_none(&self) -> bool {
        matches!(self, FileCache::None { .. })
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
                catalog_bits: config.catalog_bits,
                admissions,
                reinsertions: config.reinsertions,
                flushers: config.flushers,
                reclaimers: config.reclaimers,
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
    type Cursor = SstableBlockIndexCursor;

    fn serialized_len(&self) -> usize {
        8 + 8 // sst_id (8B) + block_idx (8B)
    }

    fn read(mut buf: &[u8]) -> CodingResult<Self> {
        let sst_id = buf.get_u64();
        let block_idx = buf.get_u64();
        Ok(Self { sst_id, block_idx })
    }

    fn into_cursor(self) -> Self::Cursor {
        SstableBlockIndexCursor::new(self)
    }
}

#[derive(Debug)]
pub struct SstableBlockIndexCursor {
    inner: SstableBlockIndex,
    pos: u8,
}

impl SstableBlockIndexCursor {
    pub fn new(inner: SstableBlockIndex) -> Self {
        Self { inner, pos: 0 }
    }
}

impl std::io::Read for SstableBlockIndexCursor {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = self.pos;
        if self.pos < 8 {
            self.pos += copy(
                &self.inner.sst_id.to_be_bytes()[self.pos as usize..],
                &mut buf,
            ) as u8;
        }
        if self.pos < 16 {
            self.pos += copy(
                &self.inner.block_idx.to_be_bytes()[self.pos as usize - 8..],
                &mut buf,
            ) as u8;
        }
        let n = (self.pos - pos) as usize;
        Ok(n)
    }
}

impl Cursor for SstableBlockIndexCursor {
    type T = SstableBlockIndex;

    fn into_inner(self) -> Self::T {
        self.inner
    }
}

/// [`CachedBlock`] uses different coding for writing to use/bypass compression.
///
/// But when reading, it will always be `Loaded`.
#[derive(Debug, Clone)]
pub enum CachedBlock {
    Loaded {
        block: Box<Block>,
    },
    Fetched {
        bytes: Bytes,
        uncompressed_capacity: usize,
    },
}

impl CachedBlock {
    pub fn should_compress(&self) -> bool {
        match self {
            CachedBlock::Loaded { .. } => true,
            // TODO(MrCroxx): based on block original compression algorithm?
            CachedBlock::Fetched { .. } => false,
        }
    }

    pub fn try_into_block(self) -> HummockResult<Box<Block>> {
        let block = match self {
            CachedBlock::Loaded { block } => block,
            // for the block was not loaded yet (refill + inflight), we need to decode it.
            // TODO(MrCroxx): avoid decode twice?
            CachedBlock::Fetched {
                bytes,
                uncompressed_capacity,
            } => {
                let block = Block::decode(bytes, uncompressed_capacity)?;
                Box::new(block)
            }
        };
        Ok(block)
    }
}

impl Value for CachedBlock {
    type Cursor = CachedBlockCursor;

    fn serialized_len(&self) -> usize {
        1 /* type */ + match self {
            CachedBlock::Loaded { block } => block.raw().len(),
            CachedBlock::Fetched { bytes, uncompressed_capacity: _ } => 8 + bytes.len(),
        }
    }

    fn read(mut buf: &[u8]) -> CodingResult<Self> {
        let v = buf.get_u8();
        let res = match v {
            0 => {
                let data = Bytes::copy_from_slice(buf);
                let block = Block::decode_from_raw(data);
                let block = Box::new(block);
                Self::Loaded { block }
            }
            1 => {
                let uncompressed_capacity = buf.get_u64() as usize;
                let data = Bytes::copy_from_slice(buf);
                let block = Block::decode(data, uncompressed_capacity)?;
                let block = Box::new(block);
                Self::Loaded { block }
            }
            _ => unreachable!(),
        };
        Ok(res)
    }

    fn into_cursor(self) -> Self::Cursor {
        CachedBlockCursor::new(self)
    }
}

#[derive(Debug)]
pub struct CachedBlockCursor {
    inner: CachedBlock,
    pos: usize,
}

impl CachedBlockCursor {
    pub fn new(inner: CachedBlock) -> Self {
        Self { inner, pos: 0 }
    }
}

impl std::io::Read for CachedBlockCursor {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = self.pos;
        match &self.inner {
            CachedBlock::Loaded { block } => {
                if self.pos < 1 {
                    self.pos += copy([0], &mut buf);
                }
                self.pos += copy(&block.raw()[self.pos - 1..], &mut buf);
            }
            CachedBlock::Fetched {
                bytes,
                uncompressed_capacity,
            } => {
                if self.pos < 1 {
                    self.pos += copy([1], &mut buf);
                }
                if self.pos < 9 {
                    self.pos += copy(
                        &uncompressed_capacity.to_be_bytes()[self.pos - 1..],
                        &mut buf,
                    );
                }
                self.pos += copy(&bytes[self.pos - 9..], &mut buf);
            }
        }
        let n = self.pos - pos;
        Ok(n)
    }
}

impl Cursor for CachedBlockCursor {
    type T = CachedBlock;

    fn into_inner(self) -> Self::T {
        self.inner
    }
}

impl Value for Box<Block> {
    type Cursor = BoxBlockCursor;

    fn serialized_len(&self) -> usize {
        self.raw().len()
    }

    fn read(buf: &[u8]) -> CodingResult<Self> {
        let data = Bytes::copy_from_slice(buf);
        let block = Block::decode_from_raw(data);
        let block = Box::new(block);
        Ok(block)
    }

    fn into_cursor(self) -> Self::Cursor {
        BoxBlockCursor::new(self)
    }
}

#[derive(Debug)]
pub struct BoxBlockCursor {
    inner: Box<Block>,
    pos: usize,
}

impl BoxBlockCursor {
    pub fn new(inner: Box<Block>) -> Self {
        Self { inner, pos: 0 }
    }
}

impl std::io::Read for BoxBlockCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = self.pos;
        self.pos += copy(&self.inner.raw()[self.pos..], buf);
        let n = self.pos - pos;
        Ok(n)
    }
}

impl Cursor for BoxBlockCursor {
    type T = Box<Block>;

    fn into_inner(self) -> Self::T {
        self.inner
    }
}

#[derive(Debug)]
pub struct CachedSstable(Arc<Sstable>);

impl Clone for CachedSstable {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl From<Box<Sstable>> for CachedSstable {
    fn from(value: Box<Sstable>) -> Self {
        Self(Arc::new(*value))
    }
}

impl From<CachedSstable> for Box<Sstable> {
    fn from(value: CachedSstable) -> Self {
        Box::new(Arc::unwrap_or_clone(value.0))
    }
}

impl CachedSstable {
    pub fn into_inner(self) -> Arc<Sstable> {
        self.0
    }
}

impl Value for CachedSstable {
    type Cursor = CachedSstableCursor;

    fn weight(&self) -> usize {
        self.0.estimate_size()
    }

    fn serialized_len(&self) -> usize {
        8 + self.0.meta.encoded_size() // id (8B) + meta size
    }

    fn read(mut buf: &[u8]) -> CodingResult<Self> {
        let id = buf.get_u64();
        let meta = SstableMeta::decode(buf).unwrap();
        let sstable = Arc::new(Sstable::new(id, meta));
        Ok(Self(sstable))
    }

    fn into_cursor(self) -> Self::Cursor {
        CachedSstableCursor::new(self)
    }
}

#[derive(Debug)]
pub struct CachedSstableCursor {
    inner: CachedSstable,
    pos: usize,
    /// store pre-encoded bytes here, for it's hard to encode JIT
    bytes: Vec<u8>,
}

impl CachedSstableCursor {
    pub fn new(inner: CachedSstable) -> Self {
        let mut bytes = vec![];
        bytes.put_u64(inner.0.id);
        inner.0.meta.encode_to(&mut bytes);
        Self {
            inner,
            bytes,
            pos: 0,
        }
    }
}

impl std::io::Read for CachedSstableCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = self.pos;
        self.pos += copy(&self.bytes[self.pos..], buf);
        let n = self.pos - pos;
        Ok(n)
    }
}

impl Cursor for CachedSstableCursor {
    type T = CachedSstable;

    fn into_inner(self) -> Self::T {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::FullKey;

    use super::*;
    use crate::hummock::{BlockBuilder, BlockBuilderOptions, BlockMeta, CompressionAlgorithm};

    pub fn construct_full_key_struct_for_test(
        table_id: u32,
        table_key: &[u8],
        epoch: u64,
    ) -> FullKey<&[u8]> {
        FullKey::for_test(TableId::new(table_id), table_key, epoch)
    }

    fn block_for_test() -> Box<Block> {
        let options = BlockBuilderOptions {
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };

        let mut builder = BlockBuilder::new(options);
        builder.add_for_test(
            construct_full_key_struct_for_test(0, b"k1", test_epoch(1)),
            b"v01",
        );
        builder.add_for_test(
            construct_full_key_struct_for_test(0, b"k2", test_epoch(2)),
            b"v02",
        );
        builder.add_for_test(
            construct_full_key_struct_for_test(0, b"k3", test_epoch(3)),
            b"v03",
        );
        builder.add_for_test(
            construct_full_key_struct_for_test(0, b"k4", test_epoch(4)),
            b"v04",
        );

        let uncompress = builder.uncompressed_block_size();
        Box::new(Block::decode(builder.build().to_vec().into(), uncompress).unwrap())
    }

    fn sstable_for_test() -> Sstable {
        Sstable::new(
            114514,
            SstableMeta {
                block_metas: vec![
                    BlockMeta {
                        smallest_key: b"0-smallest-key".to_vec(),
                        len: 100,
                        ..Default::default()
                    },
                    BlockMeta {
                        smallest_key: b"5-some-key".to_vec(),
                        offset: 100,
                        len: 100,
                        ..Default::default()
                    },
                ],
                bloom_filter: b"0123456789012345".to_vec(),
                estimated_size: 123,
                key_count: 123,
                smallest_key: b"0-smallest-key".to_vec(),
                largest_key: b"9-largest-key".to_vec(),
                meta_offset: 123,
                monotonic_tombstone_events: vec![],
                version: 2,
            },
        )
    }

    #[test]
    fn test_cursor() {
        {
            let block = block_for_test();
            let mut cursor = block.into_cursor();
            let mut buf = vec![];
            std::io::copy(&mut cursor, &mut buf).unwrap();
            let target = cursor.into_inner();
            let block = Box::<Block>::read(&buf[..]).unwrap();
            assert_eq!(target.raw(), block.raw());
        }

        {
            let sstable: CachedSstable = Box::new(sstable_for_test()).into();
            let mut cursor = sstable.into_cursor();
            let mut buf = vec![];
            std::io::copy(&mut cursor, &mut buf).unwrap();
            let target = cursor.into_inner();
            let sstable = CachedSstable::read(&buf[..]).unwrap();
            assert_eq!(target.0.id, sstable.0.id);
            assert_eq!(target.0.meta, sstable.0.meta);
        }

        {
            let cached = CachedBlock::Loaded {
                block: block_for_test(),
            };
            let mut cursor = cached.into_cursor();
            let mut buf = vec![];
            std::io::copy(&mut cursor, &mut buf).unwrap();
            let target = cursor.into_inner();
            let cached = CachedBlock::read(&buf[..]).unwrap();
            let target = match target {
                CachedBlock::Loaded { block } => block,
                CachedBlock::Fetched { .. } => panic!(),
            };
            let block = match cached {
                CachedBlock::Loaded { block } => block,
                CachedBlock::Fetched { .. } => panic!(),
            };
            assert_eq!(target.raw(), block.raw());
        }

        {
            let index = SstableBlockIndex {
                sst_id: 114,
                block_idx: 514,
            };
            let mut cursor = index.into_cursor();
            let mut buf = vec![];
            std::io::copy(&mut cursor, &mut buf).unwrap();
            let target = cursor.into_inner();
            let index = SstableBlockIndex::read(&buf[..]).unwrap();
            assert_eq!(target, index);
        }
    }
}
