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

use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId, HummockSstableId};
use thiserror::Error;

use crate::model::MetadataModelError;
use crate::storage::MetaStoreError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid hummock context {0}")]
    InvalidContext(HummockContextId),
    #[error(transparent)]
    MetaStoreError(anyhow::Error),
    #[error("compactor {0} is disconnected")]
    CompactorUnreachable(HummockContextId),
    #[error("compaction task {0} already assigned to compactor {1}")]
    CompactionTaskAlreadyAssigned(u64, HummockContextId),
    #[error("compaction group {0} not found")]
    InvalidCompactionGroup(CompactionGroupId),
    #[error("compaction group member {0} not found")]
    InvalidCompactionGroupMember(StateTableId),
    #[error("SST {0} is invalid")]
    InvalidSst(HummockSstableId),
    #[error(transparent)]
    InternalError(anyhow::Error),
}

impl Error {
    pub fn retryable(&self) -> bool {
        matches!(self, Error::MetaStoreError(_))
    }
}

impl From<MetaStoreError> for Error {
    fn from(error: MetaStoreError) -> Self {
        match error {
            MetaStoreError::ItemNotFound(err) => anyhow::anyhow!(err).into(),
            MetaStoreError::TransactionAbort() => {
                // TODO: need more concrete error from meta store.
                Error::InvalidContext(0)
            }
            // TODO: Currently MetaStoreError::Internal is equivalent to EtcdError, which
            // includes both retryable and non-retryable. Need to expand MetaStoreError::Internal
            // to more detail meta_store errors.
            MetaStoreError::Internal(err) => Error::MetaStoreError(err),
        }
    }
}

impl From<MetadataModelError> for Error {
    fn from(err: MetadataModelError) -> Self {
        match err {
            MetadataModelError::MetaStoreError(e) => e.into(),
            e => anyhow::anyhow!(e).into(),
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::InternalError(e)
    }
}
