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

use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::config::MetaBackend;
use thiserror::Error;

use crate::storage::transaction::Transaction;
use crate::storage::{Key, Value};

pub const DEFAULT_COLUMN_FAMILY: &str = "default";

#[async_trait]
pub trait Snapshot: Sync + Send + 'static {
    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>>;
}

pub struct BoxedSnapshot(Box<dyn Snapshot + Send + Sync>);

#[async_trait]
impl Snapshot for BoxedSnapshot {
    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.deref().list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>> {
        self.0.deref().get_cf(cf, key).await
    }
}

/// `MetaStore` defines the functions used to operate metadata.
#[async_trait]
pub trait MetaStore: Sync + Send + 'static {
    type Snapshot: Snapshot;

    async fn snapshot(&self) -> Self::Snapshot;

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> MetaStoreResult<()>;
    async fn delete_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<()>;
    async fn txn(&self, trx: Transaction) -> MetaStoreResult<()>;

    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.snapshot().await.list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>> {
        self.snapshot().await.get_cf(cf, key).await
    }

    fn meta_store_type(&self) -> MetaBackend;
}

#[derive(Clone)]
pub struct MetaStoreRef(Arc<dyn MetaStore<Snapshot = BoxedSnapshot> + Send + Sync>);

pub struct BoxedSnapshotMetaStore<S: MetaStore> {
    inner: S,
}

pub trait MetaStoreBoxExt: MetaStore
where
    Self: Sized,
{
    fn into_ref(self) -> MetaStoreRef {
        MetaStoreRef(Arc::new(BoxedSnapshotMetaStore { inner: self }))
    }
}

impl<S: MetaStore> MetaStoreBoxExt for S {}

#[async_trait]
impl<S: MetaStore> MetaStore for BoxedSnapshotMetaStore<S> {
    type Snapshot = BoxedSnapshot;

    async fn snapshot(&self) -> Self::Snapshot {
        BoxedSnapshot(Box::new(self.inner.snapshot().await))
    }

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> MetaStoreResult<()> {
        self.inner.put_cf(cf, key, value).await
    }

    async fn delete_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<()> {
        self.inner.delete_cf(cf, key).await
    }

    async fn txn(&self, trx: Transaction) -> MetaStoreResult<()> {
        self.inner.txn(trx).await
    }

    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>> {
        self.inner.get_cf(cf, key).await
    }

    fn meta_store_type(&self) -> MetaBackend {
        self.inner.meta_store_type()
    }
}

#[async_trait]
impl MetaStore for MetaStoreRef {
    type Snapshot = BoxedSnapshot;

    async fn snapshot(&self) -> BoxedSnapshot {
        self.0.deref().snapshot().await
    }

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> MetaStoreResult<()> {
        self.0.deref().put_cf(cf, key, value).await
    }

    async fn delete_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<()> {
        self.0.deref().delete_cf(cf, key).await
    }

    async fn txn(&self, trx: Transaction) -> MetaStoreResult<()> {
        self.0.deref().txn(trx).await
    }

    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.deref().list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>> {
        self.0.deref().get_cf(cf, key).await
    }

    fn meta_store_type(&self) -> MetaBackend {
        self.0.deref().meta_store_type()
    }
}

// Error of metastore
#[derive(Debug, Error)]
pub enum MetaStoreError {
    #[error("item not found: {0}")]
    ItemNotFound(String),
    #[error("transaction abort")]
    TransactionAbort(),
    #[error("internal error: {0}")]
    Internal(anyhow::Error),
}

pub type MetaStoreResult<T> = std::result::Result<T, MetaStoreError>;
