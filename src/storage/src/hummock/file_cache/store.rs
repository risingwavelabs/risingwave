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

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use itertools::Itertools;
use nix::sys::statfs::{statfs, FsType as NixFsType, EXT4_SUPER_MAGIC};
use parking_lot::RwLock;
use risingwave_common::cache::{LruCache, LruCacheEventListener};
use tokio::sync::RwLock as AsyncRwLock;
use tracing::Instrument;

use super::error::{Error, Result};
use super::file::{CacheFile, CacheFileOptions};
use super::meta::{BlockLoc, MetaFile, SlotId};
use super::metrics::FileCacheMetricsRef;
use super::{utils, DioBuffer, DIO_BUFFER_ALLOCATOR};
use crate::hummock::{HashBuilder, TieredCacheKey, TieredCacheValue};

const META_FILE_FILENAME: &str = "meta";
const CACHE_FILE_FILENAME: &str = "cache";

const FREELIST_DEFAULT_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Ext4,
    Xfs,
}

pub struct StoreBatchWriter<'a, K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    keys: Vec<K>,
    buffer: DioBuffer,
    blocs: Vec<BlockLoc>,

    block_size: usize,

    store: &'a Store<K, V>,

    _phantom: PhantomData<(K, V)>,
}

impl<'a, K, V> StoreBatchWriter<'a, K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    fn new(
        store: &'a Store<K, V>,
        block_size: usize,
        buffer_capacity: usize,
        item_capacity: usize,
    ) -> Self {
        Self {
            keys: Vec::with_capacity(item_capacity),
            buffer: DioBuffer::with_capacity_in(buffer_capacity, &DIO_BUFFER_ALLOCATOR),
            blocs: Vec::with_capacity(item_capacity),

            block_size,

            store,

            _phantom: PhantomData::default(),
        }
    }

    #[allow(clippy::uninit_vec)]
    pub fn append(&mut self, key: K, value: &V) {
        let offset = self.buffer.len();
        let len = value.encoded_len();
        let bloc = BlockLoc {
            bidx: offset as u32 / self.block_size as u32,
            len: len as u32,
        };
        self.blocs.push(bloc);

        let buffer_len = utils::align_up(self.block_size, offset + len);
        self.buffer.reserve(buffer_len);
        unsafe {
            self.buffer.set_len(buffer_len);
        }
        value.encode(&mut self.buffer[offset..offset + len]);

        self.keys.push(key);
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.blocs.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[tracing::instrument(skip(self))]
    pub async fn finish(mut self) -> Result<(Vec<K>, Vec<SlotId>)> {
        // First free slots during last batch.

        let mut freelist = Vec::with_capacity(FREELIST_DEFAULT_CAPACITY);
        std::mem::swap(&mut *self.store.freelist.write(), &mut freelist);

        if !freelist.is_empty() {
            let mut guard = self
                .store
                .meta_file
                .write()
                .instrument(tracing::info_span!("meta_write_lock_free_slots"))
                .await;
            for slot in freelist {
                if let Some(bloc) = guard.free(slot) {
                    let offset = bloc.bidx as u64 * self.block_size as u64;
                    let len = bloc.blen(self.block_size as u32) as usize;
                    self.store.cache_file.punch_hole(offset, len)?;
                }
            }
            drop(guard);
        }

        if self.is_empty() {
            return Ok((vec![], vec![]));
        }

        // Write new cache entries.

        let mut slots = Vec::with_capacity(self.blocs.len());
        let len = self.buffer.len();

        let timer = self.store.metrics.disk_write_latency.start_timer();
        let boff = self.store.cache_file.append(self.buffer).await? / self.block_size as u64;
        let boff: u32 = boff.try_into().unwrap();
        timer.observe_duration();
        self.store.metrics.disk_write_bytes.inc_by(len as f64);
        self.store.metrics.disk_write_io_size.observe(len as f64);

        for bloc in &mut self.blocs {
            bloc.bidx += boff;
        }

        // Write guard is only needed when updating meta file, for data file is append-only.
        let mut guard = self
            .store
            .meta_file
            .write()
            .instrument(tracing::info_span!("meta_write_lock_update_slots"))
            .await;

        for (key, bloc) in self.keys.iter().zip_eq(self.blocs.iter()) {
            slots.push(guard.insert(key, bloc)?);
        }

        Ok((self.keys, slots))
    }
}

pub struct StoreOptions {
    pub dir: String,
    pub capacity: usize,
    pub buffer_capacity: usize,
    pub cache_file_fallocate_unit: usize,
    pub cache_meta_fallocate_unit: usize,

    pub metrics: FileCacheMetricsRef,
}

pub struct Store<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    dir: String,
    _capacity: usize,

    _fs_type: FsType,
    _fs_block_size: usize,
    block_size: usize,
    buffer_capacity: usize,

    meta_file: Arc<AsyncRwLock<MetaFile<K>>>,
    cache_file: CacheFile,

    freelist: Arc<RwLock<Vec<SlotId>>>,

    metrics: FileCacheMetricsRef,

    _phantom: PhantomData<V>,
}

impl<K, V> Store<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    pub async fn open(options: StoreOptions) -> Result<Self> {
        if !PathBuf::from(options.dir.as_str()).exists() {
            std::fs::create_dir_all(options.dir.as_str())?;
        }

        // Get file system type and block size by `statfs(2)`.
        let fs_stat = statfs(options.dir.as_str())?;
        let fs_type = match fs_stat.filesystem_type() {
            EXT4_SUPER_MAGIC => FsType::Ext4,
            // FYI: https://github.com/nix-rust/nix/issues/1742
            NixFsType(libc::XFS_SUPER_MAGIC) => FsType::Xfs,
            nix_fs_type => return Err(Error::UnsupportedFilesystem(nix_fs_type.0)),
        };
        let fs_block_size = fs_stat.block_size() as usize;

        let cf_opts = CacheFileOptions {
            // TODO: Make it configurable.
            block_size: fs_block_size,
            fallocate_unit: options.cache_file_fallocate_unit,
        };

        let mf = MetaFile::open(
            PathBuf::from(&options.dir).join(META_FILE_FILENAME),
            options.cache_meta_fallocate_unit,
        )?;

        let cf = CacheFile::open(
            PathBuf::from(&options.dir).join(CACHE_FILE_FILENAME),
            cf_opts,
        )
        .await?;

        Ok(Self {
            dir: options.dir,
            _capacity: options.capacity,

            _fs_type: fs_type,
            _fs_block_size: fs_block_size,
            // TODO: Make it configurable.
            block_size: fs_block_size,
            buffer_capacity: options.buffer_capacity,

            meta_file: Arc::new(AsyncRwLock::new(mf)),
            cache_file: cf,

            freelist: Arc::new(RwLock::new(Vec::with_capacity(FREELIST_DEFAULT_CAPACITY))),

            metrics: options.metrics,

            _phantom: PhantomData::default(),
        })
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub async fn size(&self) -> usize {
        self.cache_file.size() + self.meta_file.read().await.size()
    }

    pub async fn meta_file_size(&self) -> usize {
        self.meta_file.read().await.size()
    }

    pub fn cache_file_size(&self) -> usize {
        self.cache_file.size()
    }

    pub fn cache_file_len(&self) -> usize {
        self.cache_file.len()
    }

    pub fn meta_file_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(META_FILE_FILENAME)
    }

    pub fn cache_file_path(&self) -> PathBuf {
        PathBuf::from(&self.dir).join(CACHE_FILE_FILENAME)
    }

    pub async fn restore<S: HashBuilder>(
        &self,
        indices: &Arc<LruCache<K, SlotId>>,
        hash_builder: &S,
    ) -> Result<()> {
        let slots = self.meta_file.read().await.slots();

        for slot in 0..slots {
            // Wrap the read guard, or there will be deadlock when evicting entries.
            let res = { self.meta_file.read().await.get(slot) };
            if let Some((block_loc, key)) = res {
                indices.insert(
                    key.clone(),
                    hash_builder.hash_one(&key),
                    utils::align_up(self.block_size, block_loc.len as usize),
                    slot,
                );
            }
        }

        Ok(())
    }

    pub fn start_batch_writer(&self, item_capacity: usize) -> StoreBatchWriter<K, V> {
        StoreBatchWriter::new(self, self.block_size, self.buffer_capacity, item_capacity)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&self, slot: SlotId) -> Result<Vec<u8>> {
        // Read guard should be held during reading meta and loading data.
        let guard = self
            .meta_file
            .read()
            .instrument(tracing::info_span!("meta_file_read_lock"))
            .await;

        let (bloc, _key) = guard.get(slot).ok_or(Error::InvalidSlot(slot))?;
        let offset = bloc.bidx as u64 * self.block_size as u64;
        let blen = bloc.blen(self.block_size as u32) as usize;

        let timer = self.metrics.disk_read_latency.start_timer();
        let buf = self.cache_file.read(offset, blen).await?;
        timer.observe_duration();
        self.metrics.disk_read_bytes.inc_by(buf.len() as f64);
        self.metrics.disk_read_io_size.observe(buf.len() as f64);

        drop(guard);

        Ok(buf[..bloc.len as usize].to_vec())
    }

    pub fn erase(&self, slot: SlotId) -> Result<()> {
        self.free(slot)
    }

    fn free(&self, slot: SlotId) -> Result<()> {
        // `free` is called by `CacheableEntry::drop` which cannot be async, so delay actual free
        // until next batch write.

        // TODO(MrCroxx): Optimize freelist to use lock-free queue with batching read.
        self.freelist.write().push(slot);
        Ok(())
    }
}

impl<K, V> LruCacheEventListener for Store<K, V>
where
    K: TieredCacheKey,
    V: TieredCacheValue,
{
    type K = K;
    type T = SlotId;

    fn on_release(&self, _key: Self::K, slot: Self::T) {
        // TODO: Throw warning log instead?
        self.free(slot).unwrap();
    }
}

pub type StoreRef<K, V> = Arc<Store<K, V>>;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::hummock::file_cache::test_utils::{TestCacheKey, TestCacheValue};

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<StoreRef<TestCacheKey, TestCacheValue>>();
    }
}
