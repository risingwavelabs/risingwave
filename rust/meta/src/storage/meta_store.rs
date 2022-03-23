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

use std::str;

use async_trait::async_trait;
use risingwave_common::error::{ErrorCode, RwError};

use crate::storage::transaction::Transaction;
use crate::storage::{Key, Value};

pub const DEFAULT_COLUMN_FAMILY: &str = "default";

#[async_trait]
pub trait Snapshot: Sync + Send + 'static {
    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>>;
    async fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Vec<u8>>;
}

/// `MetaStore` defines the functions used to operate metadata.
#[async_trait]
pub trait MetaStore: Clone + Sync + Send + 'static {
    type Snapshot: Snapshot;

    async fn snapshot(&self) -> Self::Snapshot;

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> Result<()>;
    async fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()>;
    async fn txn(&self, trx: Transaction) -> Result<()>;

    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>> {
        self.snapshot().await.list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.snapshot().await.get_cf(cf, key).await
    }
}

// Error of metastore
#[derive(Debug)]
pub enum Error {
    ItemNotFound(String),
    TransactionAbort(),
    Internal(anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for RwError {
    fn from(e: Error) -> Self {
        match e {
            Error::ItemNotFound(k) => RwError::from(ErrorCode::ItemNotFound(hex::encode(k))),
            Error::TransactionAbort() => {
                RwError::from(ErrorCode::InternalError("transaction aborted".to_owned()))
            }
            Error::Internal(e) => RwError::from(ErrorCode::InternalError(format!(
                "meta internal error: {}",
                e
            ))),
        }
    }
}
