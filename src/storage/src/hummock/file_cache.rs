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
use foyer::storage::store::PrometheusConfig;
use foyer::storage::LfuFsStoreConfig;
use prometheus::Registry;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_hummock_sdk::HummockSstableObjectId;
use tokio::sync::{mpsc, oneshot};

use super::{Block, Sstable};
use crate::hummock::SstableMeta;

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

pub struct FoyerStoreConfig {
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
}

pub struct FoyerRuntimeConfig {
    pub foyer_store_config: FoyerStoreConfig,
    pub runtime_worker_threads: Option<usize>,
}

#[derive(Debug)]
pub enum FoyerRuntimeTask<K, V>
where
    K: Key + Copy,
    V: Value,
{
    Insert {
        key: K,
        value: V,
        tx: Option<oneshot::Sender<Result<()>>>,
    },
    Remove {
        key: K,
        tx: Option<oneshot::Sender<Result<()>>>,
    },
    Clear {
        tx: Option<oneshot::Sender<Result<()>>>,
    },
    Lookup {
        key: K,
        tx: oneshot::Sender<Result<Option<V>>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
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

impl Value for Box<Sstable> {
    fn weight(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn serialized_len(&self) -> usize {
        8 + self.meta.encoded_size()
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
    Foyer(Arc<FoyerStore<K, V>>),
    FoyerRuntime {
        runtime: Arc<BackgroundShutdownRuntime>,
        task_tx: Arc<mpsc::UnboundedSender<FoyerRuntimeTask<K, V>>>,
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

    pub async fn foyer(config: FoyerStoreConfig) -> Result<Self> {
        let file_capacity = config.file_capacity;
        let capacity = config.capacity;
        let capacity = capacity - (capacity % file_capacity);

        let mut admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>> = vec![];
        if config.rated_random_rate > 0 {
            let rr = RatedRandom::new(config.rated_random_rate, Duration::from_millis(100));
            admissions.push(Arc::new(rr));
        }

        let c = LfuFsStoreConfig {
            eviction_config: EvictionConfig {
                window_to_cache_size_ratio: config.lfu_window_to_cache_size_ratio,
                tiny_lru_capacity_ratio: config.lfu_tiny_lru_capacity_ratio,
            },
            device_config: DeviceConfig {
                dir: config.dir,
                capacity,
                file_capacity,
                align: config.device_align,
                io_size: config.device_io_size,
            },
            admissions,
            reinsertions: vec![],
            buffer_pool_size: config.buffer_pool_size,
            flushers: config.flushers,
            flush_rate_limit: config.flush_rate_limit,
            reclaimers: config.reclaimers,
            reclaim_rate_limit: config.reclaim_rate_limit,
            recover_concurrency: config.recover_concurrency,
            prometheus_config: PrometheusConfig {
                registry: config.prometheus_registry,
                namespace: config.prometheus_namespace,
            },
        };
        let store = FoyerStore::open(c).await.map_err(FileCacheError::foyer)?;
        Ok(Self::Foyer(store))
    }

    pub async fn foyer_runtime(config: FoyerRuntimeConfig) -> Result<Self> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(runtime_worker_threads) = config.runtime_worker_threads {
            builder.worker_threads(runtime_worker_threads);
        }
        let runtime = builder
            .thread_name("risingwave-foyer-storage")
            .enable_all()
            .build()
            .map_err(|e| FileCacheError::Other(e.into()))?;
        let (task_tx, mut task_rx) = mpsc::unbounded_channel();

        let (tx, rx) = oneshot::channel();
        runtime.spawn(async move {
            let foyer_store_config = config.foyer_store_config;

            let file_capacity = foyer_store_config.file_capacity;
            let capacity = foyer_store_config.capacity;
            let capacity = capacity - (capacity % file_capacity);

            let mut admissions: Vec<Arc<dyn AdmissionPolicy<Key = K, Value = V>>> = vec![];
            if foyer_store_config.rated_random_rate > 0 {
                let rr = RatedRandom::new(
                    foyer_store_config.rated_random_rate,
                    Duration::from_millis(100),
                );
                admissions.push(Arc::new(rr));
            }

            let c = LfuFsStoreConfig {
                eviction_config: EvictionConfig {
                    window_to_cache_size_ratio: foyer_store_config.lfu_window_to_cache_size_ratio,
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
                prometheus_config: PrometheusConfig {
                    registry: foyer_store_config.prometheus_registry,
                    namespace: foyer_store_config.prometheus_namespace,
                },
            };
            match FoyerStore::open(c).await.map_err(FileCacheError::foyer) {
                Err(e) => tx.send(Err(e)).unwrap(),
                Ok(store) => {
                    tx.send(Ok(())).unwrap();
                    while let Some(task) = task_rx.recv().await {
                        match task {
                            FoyerRuntimeTask::Insert { key, value, tx } => {
                                let res = store
                                    .insert(key, value)
                                    .await
                                    .map(|_| ())
                                    .map_err(FileCacheError::foyer);
                                if let Some(tx) = tx {
                                    tx.send(res).unwrap();
                                }
                            }
                            FoyerRuntimeTask::Remove { key, tx } => {
                                store.remove(&key);
                                if let Some(tx) = tx {
                                    tx.send(Ok(())).unwrap();
                                }
                            }
                            FoyerRuntimeTask::Clear { tx } => {
                                store.clear();
                                if let Some(tx) = tx {
                                    tx.send(Ok(())).unwrap();
                                }
                            }
                            FoyerRuntimeTask::Lookup { key, tx } => {
                                let res = store.lookup(&key).await.map_err(FileCacheError::foyer);
                                tx.send(res).unwrap();
                            }
                        }
                    }
                }
            };
        });

        rx.await.unwrap()?;

        Ok(Self::FoyerRuntime {
            runtime: Arc::new(runtime.into()),
            task_tx: Arc::new(task_tx),
        })
    }

    #[tracing::instrument(skip(self, value))]
    pub async fn insert(&self, key: K, value: V) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => store
                .insert(key, value)
                .await
                .map(|_| ())
                .map_err(FileCacheError::foyer),
            FileCache::FoyerRuntime { task_tx, .. } => {
                let (tx, rx) = oneshot::channel();
                task_tx
                    .send(FoyerRuntimeTask::Insert {
                        key,
                        value,
                        tx: Some(tx),
                    })
                    .unwrap();
                rx.await.unwrap()
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn insert_without_wait(&self, key: K, value: V) {
        match self {
            FileCache::None => {}
            FileCache::Foyer(_) => panic!("unsupported"),
            FileCache::FoyerRuntime { task_tx, .. } => {
                task_tx
                    .send(FoyerRuntimeTask::Insert {
                        key,
                        value,
                        tx: None,
                    })
                    .unwrap();
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn remove(&self, key: &K) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => {
                store.remove(key);
                Ok(())
            }
            FileCache::FoyerRuntime { task_tx, .. } => {
                let (tx, rx) = oneshot::channel();
                task_tx
                    .send(FoyerRuntimeTask::Remove {
                        key: *key,
                        tx: Some(tx),
                    })
                    .unwrap();
                rx.await.unwrap()
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn remove_without_wait(&self, key: &K) {
        match self {
            FileCache::None => {}
            FileCache::Foyer(_) => panic!("unsupported"),
            FileCache::FoyerRuntime { task_tx, .. } => {
                task_tx
                    .send(FoyerRuntimeTask::Remove {
                        key: *key,
                        tx: None,
                    })
                    .unwrap();
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear(&self) -> Result<()> {
        match self {
            FileCache::None => Ok(()),
            FileCache::Foyer(store) => {
                store.clear();
                Ok(())
            }
            FileCache::FoyerRuntime { task_tx, .. } => {
                let (tx, rx) = oneshot::channel();
                task_tx
                    .send(FoyerRuntimeTask::Clear { tx: Some(tx) })
                    .unwrap();
                rx.await.unwrap()
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn clear_without_wait(&self) {
        match self {
            FileCache::None => {}
            FileCache::Foyer(_) => panic!("unsupported"),
            FileCache::FoyerRuntime { task_tx, .. } => {
                task_tx.send(FoyerRuntimeTask::Clear { tx: None }).unwrap();
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn lookup(&self, key: &K) -> Result<Option<V>> {
        match self {
            FileCache::None => Ok(None),
            FileCache::Foyer(store) => store.lookup(key).await.map_err(FileCacheError::foyer),
            FileCache::FoyerRuntime { task_tx, .. } => {
                let (tx, rx) = oneshot::channel();
                task_tx
                    .send(FoyerRuntimeTask::Lookup { key: *key, tx })
                    .unwrap();
                rx.await.unwrap()
            }
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
