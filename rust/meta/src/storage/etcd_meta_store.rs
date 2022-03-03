use std::sync::atomic::{self, AtomicI64};

use anyhow;
use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, Error as EtcdError, GetOptions, KvClient, Txn, TxnOp,
};
use futures::Future;
use tokio::sync::Mutex;

use super::{Error, Key, MetaStore, Result, Snapshot, Transaction, Value};

impl From<EtcdError> for Error {
    fn from(err: EtcdError) -> Self {
        Error::Internal(anyhow::Error::new(err))
    }
}

const REVISION_UNINITIALIZED: i64 = -1;

#[derive(Clone)]
pub struct EtcdMetaStore {
    client: Client,
}
pub struct EtcdSnapshot {
    client: KvClient,
    revision: AtomicI64,
    init_lock: Mutex<()>,
}

// TODO: we can refine the key encoding before release.
fn encode_etcd_key(cf: &str, key: &[u8]) -> Vec<u8> {
    let mut encoded_key = Vec::with_capacity(key.len() + cf.len() + 1);
    encoded_key.extend_from_slice(cf.as_bytes());
    encoded_key.push(b'/');
    encoded_key.extend_from_slice(key);
    encoded_key
}

impl EtcdSnapshot {
    async fn view_inner<V: SnapshotViewer>(&self, view: V) -> Result<V::Output> {
        loop {
            let revision = self.revision.load(atomic::Ordering::Relaxed);
            if revision != REVISION_UNINITIALIZED {
                // Fast and likely path.
                let (_, output) = view.view(self.client.clone(), revision).await?;
                return Ok(output);
            } else {
                // Slow path
                let _g = self.init_lock.lock().await;
                let revision = self.revision.load(atomic::Ordering::Relaxed);
                if revision != REVISION_UNINITIALIZED {
                    // Double check failed, release the lock.
                    continue;
                }
                let (new_revision, output) = view.view(self.client.clone(), revision).await?;
                self.revision.store(new_revision, atomic::Ordering::Release);
                return Ok(output);
            }
        }
    }
}

trait SnapshotViewer {
    type Output;
    type OutputFuture<'a>: Future<Output = Result<(i64, Self::Output)>> + 'a
    where
        Self: 'a;

    fn view(&self, client: KvClient, revision: i64) -> Self::OutputFuture<'_>;
}

struct GetViewer {
    key: Vec<u8>,
}

impl SnapshotViewer for GetViewer {
    type Output = Vec<u8>;
    type OutputFuture<'a> = impl Future<Output = Result<(i64, Self::Output)>> + 'a;

    fn view(&self, mut client: KvClient, revision: i64) -> Self::OutputFuture<'_> {
        async move {
            let res = client
                .get(
                    self.key.clone(),
                    Some(GetOptions::default().with_revision(revision)),
                )
                .await?;
            let new_revision = if let Some(header) = res.header() {
                header.revision()
            } else {
                return Err(Error::Internal(anyhow::anyhow!(
                    "Etcd response missing header"
                )));
            };
            let value = res
                .kvs()
                .first()
                .map(|kv| kv.value().to_vec())
                .ok_or_else(|| Error::ItemNotFound(hex::encode(self.key.clone())))?;
            Ok((new_revision, value))
        }
    }
}

struct ListViewer {
    key: Vec<u8>,
}

impl SnapshotViewer for ListViewer {
    type Output = Vec<Vec<u8>>;
    type OutputFuture<'a> = impl Future<Output = Result<(i64, Self::Output)>> + 'a;

    fn view(&self, mut client: KvClient, revision: i64) -> Self::OutputFuture<'_> {
        async move {
            let res = client
                .get(
                    self.key.clone(),
                    Some(GetOptions::default().with_revision(revision).with_prefix()),
                )
                .await?;
            let new_revision = if let Some(header) = res.header() {
                header.revision()
            } else {
                return Err(Error::Internal(anyhow::anyhow!(
                    "Etcd response missing header"
                )));
            };
            let value = res.kvs().iter().map(|kv| kv.value().to_vec()).collect();
            Ok((new_revision, value))
        }
    }
}

#[async_trait]
impl Snapshot for EtcdSnapshot {
    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>> {
        let view = ListViewer {
            key: encode_etcd_key(cf, &[]),
        };
        self.view_inner(view).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        let view = GetViewer {
            key: encode_etcd_key(cf, key),
        };
        self.view_inner(view).await
    }
}

impl EtcdMetaStore {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MetaStore for EtcdMetaStore {
    type Snapshot = EtcdSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        EtcdSnapshot {
            client: self.client.kv_client(),
            revision: AtomicI64::new(REVISION_UNINITIALIZED),
            init_lock: Default::default(),
        }
    }

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> Result<()> {
        self.client
            .kv_client()
            .put(encode_etcd_key(cf, &key), value, None)
            .await?;
        Ok(())
    }

    async fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.client
            .kv_client()
            .delete(encode_etcd_key(cf, key), None)
            .await?;
        Ok(())
    }

    async fn txn(&self, trx: Transaction) -> Result<()> {
        let (preconditions, operations) = trx.into_parts();
        let when = preconditions
            .into_iter()
            .map(|cond| match cond {
                super::Precondition::KeyExists { cf, key } => {
                    Compare::value(encode_etcd_key(&cf, &key), CompareOp::Equal, vec![])
                }
            })
            .collect::<Vec<_>>();

        let then = operations
            .into_iter()
            .map(|op| match op {
                super::Operation::Put { cf, key, value } => {
                    let key = encode_etcd_key(&cf, &key);
                    let value = value.to_vec();
                    TxnOp::put(key, value, None)
                }
                super::Operation::Delete { cf, key } => {
                    let key = encode_etcd_key(&cf, &key);
                    TxnOp::delete(key, None)
                }
            })
            .collect::<Vec<_>>();

        let etcd_txn = Txn::new().when(when).and_then(then);
        self.client.kv_client().txn(etcd_txn).await?;
        Ok(())
    }
}
