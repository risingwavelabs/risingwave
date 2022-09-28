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

use std::backtrace::Backtrace;
use std::sync::Arc;

use risingwave_pb::ProstFieldNotFound;
use risingwave_rpc_client::error::RpcError;

use crate::hummock::error::Error as HummockError;
use crate::manager::WorkerId;
use crate::model::MetadataModelError;
use crate::storage::MetaStoreError;

pub type MetaResult<T> = std::result::Result<T, MetaError>;

#[derive(thiserror::Error, Debug)]
enum MetaErrorInner {
    #[error("MetaStore transaction error: {0}")]
    TransactionError(MetaStoreError),

    #[error("MetadataModel error: {0}")]
    MetadataModelError(MetadataModelError),

    #[error("Hummock error: {0}")]
    HummockError(HummockError),

    #[error("Rpc error: {0}")]
    RpcError(RpcError),

    #[error("PermissionDenied: {0}")]
    PermissionDenied(String),

    #[error("Invalid worker: {0}")]
    InvalidWorker(WorkerId),

    // Used for catalog errors.
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),

    #[error("{0} with name {1} exists")]
    Duplicated(&'static str, String),

    #[error(transparent)]
    Internal(anyhow::Error),
}

impl From<MetaErrorInner> for MetaError {
    fn from(inner: MetaErrorInner) -> Self {
        Self {
            inner: Arc::new(inner),
            backtrace: Arc::new(Backtrace::capture()),
        }
    }
}

#[derive(thiserror::Error, Clone)]
#[error("{inner}")]
pub struct MetaError {
    inner: Arc<MetaErrorInner>,
    backtrace: Arc<Backtrace>,
}

impl std::fmt::Debug for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(f, "  backtrace of `MetaError`:\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl MetaError {
    /// Permission denied error.
    pub fn permission_denied(s: String) -> Self {
        MetaErrorInner::PermissionDenied(s).into()
    }

    pub fn invalid_worker(worker_id: WorkerId) -> Self {
        MetaErrorInner::InvalidWorker(worker_id).into()
    }

    pub fn is_invalid_worker(&self) -> bool {
        use std::borrow::Borrow;
        std::matches!(self.inner.borrow(), &MetaErrorInner::InvalidWorker(_))
    }

    pub fn catalog_not_found<T: ToString>(relation: &'static str, name: T) -> Self {
        MetaErrorInner::NotFound(relation, name.to_string()).into()
    }

    pub fn catalog_duplicated<T: ToString>(relation: &'static str, name: T) -> Self {
        MetaErrorInner::Duplicated(relation, name.to_string()).into()
    }
}

impl From<MetadataModelError> for MetaError {
    fn from(e: MetadataModelError) -> Self {
        MetaErrorInner::MetadataModelError(e).into()
    }
}

impl From<HummockError> for MetaError {
    fn from(e: HummockError) -> Self {
        MetaErrorInner::HummockError(e).into()
    }
}

impl From<RpcError> for MetaError {
    fn from(e: RpcError) -> Self {
        MetaErrorInner::RpcError(e).into()
    }
}

impl From<anyhow::Error> for MetaError {
    fn from(a: anyhow::Error) -> Self {
        MetaErrorInner::Internal(a).into()
    }
}

impl From<MetaError> for tonic::Status {
    fn from(err: MetaError) -> Self {
        match &*err.inner {
            MetaErrorInner::PermissionDenied(_) => {
                tonic::Status::permission_denied(err.to_string())
            }
            MetaErrorInner::NotFound(_, _) => tonic::Status::not_found(err.to_string()),
            MetaErrorInner::Duplicated(_, _) => tonic::Status::already_exists(err.to_string()),
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}

impl From<ProstFieldNotFound> for MetaError {
    fn from(e: ProstFieldNotFound) -> Self {
        MetadataModelError::from(e).into()
    }
}

impl From<MetaStoreError> for MetaError {
    fn from(e: MetaStoreError) -> Self {
        match e {
            // `MetaStore::txn` method error.
            MetaStoreError::TransactionAbort() => MetaErrorInner::TransactionError(e).into(),
            _ => MetadataModelError::from(e).into(),
        }
    }
}
