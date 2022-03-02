use std::fmt::Debug;
use std::sync::Arc;

use moka::future::Cache;
use risingwave_common::array::RwError;
use risingwave_common::error::Result;
use risingwave_rpc_client::MetaClient;

use crate::hummock::hummock_meta_client::RpcHummockMetaClient;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::HummockStateStore;
use crate::memory::MemoryStateStore;
use crate::monitor::{MonitoredStateStore as Monitored, StateStoreStats};
use crate::object::S3ObjectStore;
use crate::rocksdb_local::RocksDBStateStore;
use crate::tikv::TikvStateStore;
use crate::StateStore;

/// The type erased [`StateStore`].
#[derive(Clone)]
pub enum StateStoreImpl {
    HummockStateStore(Monitored<HummockStateStore>),
    MemoryStateStore(Monitored<MemoryStateStore>),
    RocksDBStateStore(Monitored<RocksDBStateStore>),
    TikvStateStore(Monitored<TikvStateStore>),
}

impl StateStoreImpl {
    pub fn shared_in_memory_store() -> Self {
        use crate::monitor::DEFAULT_STATE_STORE_STATS;

        Self::MemoryStateStore(
            MemoryStateStore::shared().monitored(DEFAULT_STATE_STORE_STATS.clone()),
        )
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_hummock) => write!(f, "HummockStateStore(_)"),
            StateStoreImpl::MemoryStateStore(_memory) => write!(f, "MemoryStateStore(_)"),
            StateStoreImpl::RocksDBStateStore(_rocksdb) => write!(f, "RocksDBStateStore(_)"),
            StateStoreImpl::TikvStateStore(_tikv) => write!(f, "TikvStateStore(_)"),
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
    pub async fn from_str(
        s: &str,
        meta_client: MetaClient,
        stats: Arc<StateStoreStats>,
    ) -> Result<Self> {
        let store = match s {
            "in_memory" | "in-memory" => StateStoreImpl::shared_in_memory_store(),

            tikv if tikv.starts_with("tikv") => {
                let inner =
                    TikvStateStore::new(vec![tikv.strip_prefix("tikv://").unwrap().to_string()]);
                StateStoreImpl::TikvStateStore(inner.monitored(stats))
            }

            minio if minio.starts_with("hummock+minio://") => {
                use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

                use crate::hummock::{HummockOptions, HummockStorage};
                // TODO: initialize those settings in a yaml file or command line instead of
                // hard-coding (#2165).
                let object_client = Arc::new(
                    S3ObjectStore::new_with_minio(minio.strip_prefix("hummock+").unwrap()).await,
                );
                let remote_dir = "hummock_001";
                let inner = HummockStateStore::new(
                    HummockStorage::new(
                        object_client.clone(),
                        HummockOptions {
                            sstable_size: 256 * (1 << 20),
                            block_size: 64 * (1 << 10),
                            bloom_false_positive: 0.1,
                            remote_dir: remote_dir.to_string(),
                            checksum_algo: ChecksumAlg::Crc32c,
                        },
                        Arc::new(LocalVersionManager::new(
                            object_client,
                            remote_dir,
                            // TODO: configurable block cache in config
                            // 1GB block cache (65536 blocks * 64KB block)
                            Some(Arc::new(Cache::new(65536))),
                        )),
                        Arc::new(RpcHummockMetaClient::new(meta_client, stats.clone())),
                        stats.clone(),
                    )
                    .await
                    .map_err(RwError::from)?,
                );
                StateStoreImpl::HummockStateStore(inner.monitored(stats))
            }

            s3 if s3.starts_with("hummock+s3://") => {
                use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

                use crate::hummock::{HummockOptions, HummockStorage};
                let s3_store = Arc::new(
                    S3ObjectStore::new(s3.strip_prefix("hummock+s3://").unwrap().to_string()).await,
                );

                let remote_dir = "hummock_001";
                let inner = HummockStateStore::new(
                    HummockStorage::new(
                        s3_store.clone(),
                        HummockOptions {
                            sstable_size: 256 * (1 << 20),
                            block_size: 64 * (1 << 10),
                            bloom_false_positive: 0.1,
                            remote_dir: remote_dir.to_string(),
                            checksum_algo: ChecksumAlg::Crc32c,
                        },
                        Arc::new(LocalVersionManager::new(
                            s3_store,
                            remote_dir,
                            Some(Arc::new(Cache::new(65536))),
                        )),
                        Arc::new(RpcHummockMetaClient::new(meta_client, stats.clone())),
                        stats.clone(),
                    )
                    .await
                    .map_err(RwError::from)?,
                );
                StateStoreImpl::HummockStateStore(inner.monitored(stats))
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
