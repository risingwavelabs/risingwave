use std::str;

use async_trait::async_trait;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode;

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
pub trait MetaStore: Sync + Send + 'static {
    type Snapshot: Snapshot;

    fn snapshot(&self) -> Self::Snapshot;

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> Result<()>;
    async fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()>;
    async fn txn(&self, trx: Transaction) -> Result<()>;

    /// We will need a proper implementation for `list`, `list_cf` in etcd
    /// [`MetaStore`]. In a naive implementation, we need to select the latest version for each key
    /// locally after fetching all versions of it from etcd, which may not meet our
    /// performance expectation.
    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>> {
        self.snapshot().list_cf(cf).await
    }

    async fn get_cf(&self, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        self.snapshot().get_cf(cf, key).await
    }
}

// Error of metastore
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    ItemNotFound(String),
    TransactionAbort(),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for RwError {
    fn from(e: Error) -> Self {
        match e {
            Error::ItemNotFound(k) => RwError::from(ErrorCode::ItemNotFound(hex::encode(k))),
            Error::TransactionAbort() => {
                RwError::from(ErrorCode::InternalError("transaction aborted".to_owned()))
            }
        }
    }
}
