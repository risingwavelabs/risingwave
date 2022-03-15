use std::fmt::Debug;
use std::sync::Arc;

use risingwave_common::config::StorageConfig;
use risingwave_common::error::{Result, RwError};
use risingwave_rpc_client::MetaClient;

use crate::hummock::hummock_meta_client::RpcHummockMetaClient;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::{HummockStateStore, SstableStore};
use crate::memory::MemoryStateStore;
use crate::monitor::{MonitoredStateStore as Monitored, StateStoreMetrics};
use crate::object::{InMemObjectStore, ObjectStore, S3ObjectStore};
use crate::rocksdb_local::RocksDBStateStore;
use crate::tikv::TikvStateStore;
use crate::StateStore;

/// The type erased [`StateStore`].
#[derive(Clone)]
pub enum StateStoreImpl {
    /// The Hummock state store, which operates on an S3-like service. URLs beginning with
    /// `hummock` will be automatically recognized as Hummock state store.
    ///
    /// Example URLs:
    ///
    /// * `hummock+s3://bucket`
    /// * `hummock+minio://KEY:SECRET@minio-ip:port`
    /// * `hummock+memory` (should only be used in 1 compute node mode)
    HummockStateStore(Monitored<HummockStateStore>),
    /// In-memory B-Tree state store. Should only be used in unit and integration tests. If you
    /// want speed up e2e test, you should use Hummock in-memory mode instead. Also, this state
    /// store misses some critical implementation to ensure the correctness of persisting streaming
    /// state. (e.g., no read_epoch support, no async checkpoint)
    MemoryStateStore(Monitored<MemoryStateStore>),
    /// Should enable `rocksdb-local` feature to use this state store. Not feature-complete, and
    /// should never be used in tests and production.
    RocksDBStateStore(Monitored<RocksDBStateStore>),
    /// Should enable `tikv` feature to use this state store. Not feature-complete, and
    /// should never be used in tests and production.
    TikvStateStore(Monitored<TikvStateStore>),
}

impl StateStoreImpl {
    pub fn shared_in_memory_store(state_store_metrics: Arc<StateStoreMetrics>) -> Self {
        Self::MemoryStateStore(MemoryStateStore::shared().monitored(state_store_metrics))
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_) => write!(f, "HummockStateStore"),
            StateStoreImpl::MemoryStateStore(_) => write!(f, "MemoryStateStore"),
            StateStoreImpl::RocksDBStateStore(_) => write!(f, "RocksDBStateStore"),
            StateStoreImpl::TikvStateStore(_) => write!(f, "TikvStateStore"),
        }
    }
}

#[macro_export]
macro_rules! dispatch_state_store {
    ($impl:expr, $store:ident, $body:tt) => {
        match $impl {
            StateStoreImpl::MemoryStateStore($store) => $body,
            StateStoreImpl::HummockStateStore($store) => $body,
            StateStoreImpl::TikvStateStore($store) => $body,
            StateStoreImpl::RocksDBStateStore($store) => $body,
        }
    };
}

impl StateStoreImpl {
    pub async fn new(
        s: &str,
        config: Arc<StorageConfig>,
        meta_client: MetaClient,
        stats: Arc<StateStoreMetrics>,
    ) -> Result<Self> {
        let store = match s {
            hummock if hummock.starts_with("hummock") => {
                use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

                use crate::hummock::{HummockOptions, HummockStorage};

                let object_store = match hummock {
                    s3 if s3.starts_with("hummock+s3://") => Arc::new(
                        S3ObjectStore::new(s3.strip_prefix("hummock+s3://").unwrap().to_string())
                            .await,
                    )
                        as Arc<dyn ObjectStore>,
                    minio if minio.starts_with("hummock+minio://") => Arc::new(
                        S3ObjectStore::new_with_minio(minio.strip_prefix("hummock+").unwrap())
                            .await,
                    )
                        as Arc<dyn ObjectStore>,
                    memory if memory.starts_with("hummock+memory") => {
                        tracing::warn!("You're using Hummock in-memory object store. This should never be used in benchmarks and production environment.");
                        Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>
                    }
                    other => {
                        unimplemented!(
                            "{} Hummock only supports s3, minio and memory for now.",
                            other
                        )
                    }
                };

                let sstable_store = Arc::new(SstableStore::new(
                    object_store,
                    config.data_directory.to_string(),
                ));
                let inner = HummockStateStore::new(
                    HummockStorage::new(
                        HummockOptions {
                            sstable_size: config.sstable_size,
                            block_size: config.block_size,
                            bloom_false_positive: config.bloom_false_positive,
                            data_directory: config.data_directory.to_string(),
                            checksum_algo: match config.checksum_algo.as_str() {
                                "crc32c" => ChecksumAlg::Crc32c,
                                "xxhash64" => ChecksumAlg::XxHash64,
                                other => {
                                    unimplemented!("{} is not supported for Hummock", other)
                                }
                            },
                            async_checkpoint_enabled: config.async_checkpoint_enabled,
                        },
                        sstable_store.clone(),
                        Arc::new(LocalVersionManager::new(sstable_store)),
                        Arc::new(RpcHummockMetaClient::new(meta_client, stats.clone())),
                        stats.clone(),
                    )
                    .await
                    .map_err(RwError::from)?,
                );
                StateStoreImpl::HummockStateStore(inner.monitored(stats))
            }

            "in_memory" | "in-memory" => {
                tracing::warn!("in-memory state backend should never be used in benchmarks and production environment.");
                StateStoreImpl::shared_in_memory_store(stats.clone())
            }

            tikv if tikv.starts_with("tikv") => {
                let inner =
                    TikvStateStore::new(vec![tikv.strip_prefix("tikv://").unwrap().to_string()]);
                StateStoreImpl::TikvStateStore(inner.monitored(stats))
            }

            rocksdb if rocksdb.starts_with("rocksdb_local://") => {
                let inner =
                    RocksDBStateStore::new(rocksdb.strip_prefix("rocksdb_local://").unwrap());
                StateStoreImpl::RocksDBStateStore(inner.monitored(stats))
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}
