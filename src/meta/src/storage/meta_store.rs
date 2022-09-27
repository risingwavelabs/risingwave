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

use async_trait::async_trait;
use thiserror::Error;

use crate::storage::transaction::Transaction;
use crate::storage::{Key, Value};

pub const DEFAULT_COLUMN_FAMILY: &str = "default";

#[async_trait]
pub trait Snapshot: Sync + Send + 'static {
    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<Vec<u8>>>;
    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>>;
}

/// `MetaStore` defines the functions used to operate metadata.
#[async_trait]
pub trait MetaStore: Clone + Sync + Send + 'static {
    type Snapshot: Snapshot;

    async fn snapshot(&self) -> Self::Snapshot;

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> MetaStoreResult<()>;
    async fn delete_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<()>;
    async fn txn(&self, trx: Transaction) -> MetaStoreResult<()>;

    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<Vec<u8>>> {
        self.snapshot().await.list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Vec<u8>> {
        self.snapshot().await.get_cf(cf, key).await
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
