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
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes};
use foyer::common::code::{Key, Value};
use foyer::storage::admission::rated_random::RatedRandomAdmissionPolicy;
use foyer::storage::admission::AdmissionPolicy;
use foyer::storage::event::EventListener;
use foyer::storage::store::{FetchValueFuture, PrometheusConfig};
use foyer::storage::LfuFsStoreConfig;
use prometheus::Registry;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_hummock_sdk::HummockSstableObjectId;

use crate::hummock::{Block, Sstable, SstableMeta};

#[derive(thiserror::Error, Debug)]
pub enum FileCacheError {
    #[error("foyer error: {0}")]
    Foyer(#[from] foyer::storage::error::Error),
    #[error("other {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl FileCacheError {
    fn foyer(e: foyer::storage::error::Error) -> Self {
        Self::Foyer(e)
    }
}

pub type Result<T> = core::result::Result<T, FileCacheError>;

pub type EvictionConfig = foyer::intrusive::eviction::lfu::LfuConfig;
pub type DeviceConfig = foyer::storage::device::fs::FsDeviceConfig;

pub type FoyerStore<K, V> = foyer::storage::LfuFsStore<K, V>;
pub type FoyerStoreResult<T> = foyer::storage::error::Result<T>;
pub type FoyerStoreError = foyer::storage::error::Error;

pub struct FoyerStoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    pub dir: PathBuf,
    pub capacity: usize,
    pub file_capacity: usize,
    pub buffer_pool_size: usize,
    pub device_align: usize,
    pub device_io_size: usize,
    pub flushers: usize,
    pub flush_rate_limit: usize,
    pub reclaimers: usize,
    pub reclaim_rate_limit: usize,
    pub recover_concurrency: usize,
    pub lfu_window_to_cache_size_ratio: usize,
    pub lfu_tiny_lru_capacity_ratio: f64,
    pub rated_random_rate: usize,
    pub prometheus_registry: Option<Registry>,
    pub prometheus_namespace: Option<String>,
    pub event_listener: Vec<Arc<dyn EventListener<K = K, V = V>>>,
    pub enable_filter: bool,
}

pub struct FoyerRuntimeConfig<K, V>
where
    K: Key,
    V: Value,
{
    pub foyer_store_config: FoyerStoreConfig<K, V>,
    pub runtime_worker_threads: Option<usize>,
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
        let meta = SstableMeta::decode(&mut buf).unwrap();
        Box::new(Sstable::new(id, meta))
    }
}

#[derive(Clone)]
pub enum FileCache<K, V>
where
    K: Key + Copy,
    V: Value,
{
    None,
    FoyerRuntime {
        runtime: Arc<BackgroundShutdownRuntime>,
        store: Arc<FoyerStore<K, V>>,
        enable_filter: bool,
    },
}

impl<K, V> FileCache<K, V>
where
    K: Key + Copy,
    V: Value,
{
    pub fn none() -> Self {
        Self::None
    }

    pub async fn foyer(config: FoyerRuntimeConfig<K, V>) -> Result<Self> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(runtime_worker_threads) = config.runtime_worker_threads {
            builder.worker_threads(runtime_worker_threads);
        }
        let runtime = builder
            .thread_name("risingwave-foyer-storage")
            .enable_all()
            .build()
            .map_err(|e| FileCacheError::Other(e.into()))?;

        let enable_filter = config.foyer_store_config.enable_filter;

        let store = runtime
            .spawn(async move {
                let foyer_store_config = config.foyer_store_config;

                let file_capacity = foyer_store_config.file_capacity;
                let capacity = foyer_store_config.capacity;
                let capacity = capacity - (capacity % file_capacity);

                let mut admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>> = vec![];
                if foyer_store_config.rated_random_rate > 0 {
                    let rr = RatedRandomAdmissionPolicy::new(
                        foyer_store_config.rated_random_rate,
                        Duration::from_millis(100),
                    );
                    admissions.push(Arc::new(rr));
                }

                let c = LfuFsStoreConfig {
                    eviction_config: EvictionConfig {
                        window_to_cache_size_ratio: foyer_store_config
                            .lfu_window_to_cache_size_ratio,
                        tiny_lru_capacity_ratio: foyer_store_config.lfu_tiny_lru_capacity_ratio,
                    },
                    device_config: DeviceConfig {
                        dir: foyer_store_config.dir.clone(),
                        capacity,
                        file_capacity,
                        align: foyer_store_config.device_align,
                        io_size: foyer_store_config.device_io_size,
                    },
                    admissions,
                    reinsertions: vec![],
                    buffer_pool_size: foyer_store_config.buffer_pool_size,
                    flushers: foyer_store_config.flushers,
                    flush_rate_limit: foyer_store_config.flush_rate_limit,
                    reclaimers: foyer_store_config.reclaimers,
                    reclaim_rate_limit: foyer_store_config.reclaim_rate_limit,
                    recover_concurrency: foyer_store_config.recover_concurrency,
                    event_listeners: foyer_store_config.event_listener,
                    prometheus_config: PrometheusConfig {
                        registry: foyer_store_config.prometheus_registry,
                        namespace: foyer_store_config.prometheus_namespace,
                    },
                    clean_region_threshold: foyer_store_config.reclaimers
                        + foyer_store_config.reclaimers / 2,
                };

                FoyerStore::open(c).await.map_err(FileCacheError::foyer)
            })
            .await
            .unwrap()?;

        Ok(Self::FoyerRuntime {
            runtime: Arc::new(runtime.into()),
            store,
            enable_filter,
        })
    }

    #[tracing::instrument(skip(self, value))]
    pub async fn insert(&self, key: K, value: V) -> Result<bool> {
        match self {
            FileCache::None => Ok(false),
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                runtime
                    .spawn(async move { store.insert_if_not_exists(key, value).await })
                    .await
                    .unwrap()
                    .map_err(FileCacheError::foyer)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn insert_without_wait(&self, key: K, value: V) {
        match self {
            FileCache::None => {}
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                runtime.spawn(async move { store.insert_if_not_exists(key, value).await });
            }
        }
    }

    /// only fetch value if judge pass
    #[tracing::instrument(skip(self, fetch_value))]
    pub async fn insert_with<F, FU>(
        &self,
        key: K,
        fetch_value: F,
        value_serialized_len: usize,
    ) -> Result<bool>
    where
        F: FnOnce() -> FU,
        FU: FetchValueFuture<V>,
    {
        match self {
            FileCache::None => Ok(false),
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                let future = fetch_value();
                runtime
                    .spawn(async move {
                        store
                            .insert_if_not_exists_with_future(
                                key,
                                || future,
                                key.serialized_len() + value_serialized_len,
                            )
                            .await
                    })
                    .await
                    .unwrap()
                    .map_err(FileCacheError::foyer)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove(&self, key: &K) -> Result<bool> {
        match self {
            FileCache::None => Ok(false),
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                let key = *key;
                runtime
                    .spawn(async move { store.remove(&key).await })
                    .await
                    .unwrap()
                    .map_err(FileCacheError::foyer)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn remove_without_wait(&self, key: &K) {
        match self {
            FileCache::None => {}
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                let key = *key;
                runtime.spawn(async move { store.remove(&key).await });
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear(&self) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                runtime
                    .spawn(async move { store.clear().await })
                    .await
                    .unwrap()
                    .map_err(FileCacheError::foyer)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn clear_without_wait(&self) {
        match self {
            FileCache::None => {}
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                runtime.spawn(async move { store.clear().await });
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn lookup(&self, key: &K) -> Result<Option<V>> {
        match self {
            FileCache::None => Ok(None),
            FileCache::FoyerRuntime { runtime, store, .. } => {
                let store = store.clone();
                let key = *key;
                runtime
                    .spawn(async move { store.lookup(&key).await })
                    .await
                    .unwrap()
                    .map_err(FileCacheError::foyer)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn exists(&self, key: &K) -> Result<bool> {
        match self {
            FileCache::None => Ok(false),
            FileCache::FoyerRuntime { store, .. } => {
                store.exists(key).map_err(FileCacheError::foyer)
            }
        }
    }

    pub fn is_filter_enabled(&self) -> bool {
        match self {
            FileCache::None => false,
            FileCache::FoyerRuntime { enable_filter, .. } => *enable_filter,
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
