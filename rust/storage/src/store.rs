use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use moka::future::Cache;
use risingwave_common::error::{Result, RwError};
use risingwave_rpc_client::MetaClient;

use crate::hummock::hummock_meta_client::RPCHummockMetaClient;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::version_manager::VersionManager;
use crate::hummock::HummockStateStore;
use crate::memory::MemoryStateStore;
use crate::rocksdb_local::RocksDBStateStore;
use crate::tikv::TikvStateStore;
use crate::write_batch::WriteBatch;

#[async_trait]
pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)>;

    /// Point get a value from the state store.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scan `limit` number of keys from the keyspace. If `limit` is `None`, scan all elements.
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    ///
    /// TODO: in some cases, the scan can be optimized into a `multi_get` request.
    async fn scan(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        collect_from_iter(self.iter(prefix).await?, limit).await
    }

    async fn reverse_scan(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        collect_from_iter(self.reverse_iter(prefix).await?, limit).await
    }

    /// Ingest a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    /// A epoch should be provided to ingest a write batch. It is served as:
    /// - A handle to represent an atomic write session. All ingested write batches associated with
    ///   the same `Epoch` have the all-or-nothing semantics, meaning that partial changes are not
    ///   queryable and will be rollbacked if instructed.
    /// - A version of a kv pair. kv pair associated with larger `Epoch` is guaranteed to be newer
    ///   then kv pair with smaller `Epoch`. Currently this version is only used to derive the
    ///   per-key modification history (e.g. in compaction), not across different keys.
    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<()>;

    /// Open and return an iterator for given `prefix`.
    async fn iter(&self, prefix: &[u8]) -> Result<Self::Iter>;

    /// Open and return a reversed iterator for given `prefix`.
    async fn reverse_iter(&self, _prefix: &[u8]) -> Result<Self::Iter> {
        unimplemented!()
    }

    /// Create a `WriteBatch` associated with this state store.
    fn start_write_batch(&self) -> WriteBatch<Self> {
        WriteBatch::new(self.clone())
    }
}

#[async_trait]
pub trait StateStoreIter: Send + 'static {
    type Item;

    async fn next(&mut self) -> Result<Option<Self::Item>>;
}

async fn collect_from_iter<I>(mut iter: I, limit: Option<usize>) -> Result<Vec<I::Item>>
where
    I: StateStoreIter,
{
    let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

    for _ in 0..limit.unwrap_or(usize::MAX) {
        match iter.next().await? {
            Some(kv) => kvs.push(kv),
            None => break,
        }
    }

    Ok(kvs)
}

#[derive(Clone)]
pub enum StateStoreImpl {
    HummockStateStore(HummockStateStore),
    MemoryStateStore(MemoryStateStore),
    RocksDBStateStore(RocksDBStateStore),
    TikvStateStore(TikvStateStore),
}

impl StateStoreImpl {
    pub fn shared_in_memory_store() -> Self {
        Self::MemoryStateStore(MemoryStateStore::shared())
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
    pub async fn from_str(s: &str, meta_client: MetaClient) -> Result<Self> {
        let store = match s {
            "in_memory" | "in-memory" => StateStoreImpl::shared_in_memory_store(),
            tikv if tikv.starts_with("tikv") => {
                StateStoreImpl::TikvStateStore(TikvStateStore::new(vec![tikv
                    .strip_prefix("tikv://")
                    .unwrap()
                    .to_string()]))
            }
            minio if minio.starts_with("hummock+minio://") => {
                use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

                use crate::hummock::{HummockOptions, HummockStorage};
                use crate::object::S3ObjectStore;
                // TODO: initialize those settings in a yaml file or command line instead of
                // hard-coding (#2165).
                let object_client = Arc::new(S3ObjectStore::new_with_minio(
                    minio.strip_prefix("hummock+").unwrap(),
                ));
                let remote_dir = "hummock_001";
                StateStoreImpl::HummockStateStore(HummockStateStore::new(
                    HummockStorage::new(
                        object_client.clone(),
                        HummockOptions {
                            sstable_size: 256 * (1 << 20),
                            block_size: 64 * (1 << 10),
                            bloom_false_positive: 0.1,
                            remote_dir: remote_dir.to_string(),
                            checksum_algo: ChecksumAlg::Crc32c,
                        },
                        Arc::new(VersionManager::new()),
                        Arc::new(LocalVersionManager::new(
                            object_client,
                            remote_dir,
                            // TODO: configurable block cache in config
                            // 1GB block cache (65536 blocks * 64KB block)
                            Some(Arc::new(Cache::new(65536))),
                        )),
                        Arc::new(RPCHummockMetaClient::new(meta_client)),
                    )
                    .await
                    .map_err(RwError::from)?,
                ))
            }
            s3 if s3.starts_with("hummock+s3://") => {
                use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;

                use crate::hummock::{HummockOptions, HummockStorage};
                use crate::object::{ConnectionInfo, S3ObjectStore};
                let s3_test_conn_info = ConnectionInfo::new();
                let s3_store = Arc::new(S3ObjectStore::new(
                    s3_test_conn_info,
                    s3.strip_prefix("hummock+s3://").unwrap().to_string(),
                ));
                let remote_dir = "hummock_001";
                StateStoreImpl::HummockStateStore(HummockStateStore::new(
                    HummockStorage::new(
                        s3_store.clone(),
                        HummockOptions {
                            sstable_size: 256 * (1 << 20),
                            block_size: 64 * (1 << 10),
                            bloom_false_positive: 0.1,
                            remote_dir: remote_dir.to_string(),
                            checksum_algo: ChecksumAlg::Crc32c,
                        },
                        Arc::new(VersionManager::new()),
                        Arc::new(LocalVersionManager::new(s3_store, remote_dir, None)),
                        Arc::new(RPCHummockMetaClient::new(meta_client)),
                    )
                    .await
                    .map_err(RwError::from)?,
                ))
            }
            rocksdb if rocksdb.starts_with("rocksdb_local://") => {
                StateStoreImpl::RocksDBStateStore(RocksDBStateStore::new(
                    rocksdb.strip_prefix("rocksdb_local://").unwrap(),
                ))
            }
            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}
