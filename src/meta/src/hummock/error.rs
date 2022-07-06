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

use risingwave_common::error::{ErrorCode, RwError, ToErrorStr};
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId};
use thiserror::Error;

use crate::storage::meta_store;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid hummock context {0}")]
    InvalidContext(HummockContextId),
    #[error(transparent)]
    MetaStoreError(anyhow::Error),
    #[error("compactor {0} is disconnected")]
    CompactorUnreachable(HummockContextId),
    #[error("compactor {0} is busy")]
    CompactorBusy(HummockContextId),
    #[error("compaction task {0} already assigned to compactor {1}")]
    CompactionTaskAlreadyAssigned(u64, HummockContextId),
    #[error("compaction group {0} not found")]
    InvalidCompactionGroup(CompactionGroupId),
    #[error("compaction group member {0} not found")]
    InvalidCompactionGroupMember(StateTableId),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl Error {
    pub fn retryable(&self) -> bool {
        matches!(self, Error::MetaStoreError(_))
    }
}

impl From<meta_store::Error> for Error {
    fn from(error: meta_store::Error) -> Self {
        match error {
            meta_store::Error::ItemNotFound(err) => Error::InternalError(err),
            meta_store::Error::TransactionAbort() => {
                // TODO: need more concrete error from meta store.
                Error::InvalidContext(0)
            }
            // TODO: Currently meta_store::Error::Internal is equivalent to EtcdError, which
            // includes both retryable and non-retryable. Need to expand meta_store::Error::Internal
            // to more detail meta_store errors.
            meta_store::Error::Internal(err) => Error::MetaStoreError(err),
        }
    }
}

impl From<Error> for ErrorCode {
    fn from(error: Error) -> Self {
        match error {
            Error::InvalidContext(err) => {
                ErrorCode::InternalError(format!("invalid hummock context {}", err))
            }
            Error::MetaStoreError(err) => ErrorCode::MetaError(err.to_error_str()),
            Error::InternalError(err) => ErrorCode::InternalError(err),
            Error::CompactorBusy(context_id) => {
                ErrorCode::InternalError(format!("compactor {} is busy", context_id))
            }
            Error::CompactorUnreachable(context_id) => {
                ErrorCode::InternalError(format!("compactor {} is unreachable", context_id))
            }
            Error::CompactionTaskAlreadyAssigned(task_id, context_id) => {
                ErrorCode::InternalError(format!(
                    "compaction task {} already assigned to compactor {}",
                    task_id, context_id
                ))
            }
            Error::InvalidCompactionGroup(group_id) => {
                ErrorCode::InternalError(format!("invalid compaction group {}", group_id))
            }
            Error::InvalidCompactionGroupMember(prefix) => {
                ErrorCode::InternalError(format!("invalid compaction group member {}", prefix))
            }
        }
    }
}

impl From<Error> for risingwave_common::error::RwError {
    fn from(error: Error) -> Self {
        ErrorCode::from(error).into()
    }
}

// TODO: as a workaround before refactoring `MetadataModel` error
impl From<risingwave_common::error::RwError> for Error {
    fn from(error: RwError) -> Self {
        match error.inner() {
            ErrorCode::InternalError(err) => Error::InternalError(err.to_owned()),
            ErrorCode::ItemNotFound(err) => Error::InternalError(err.to_owned()),
            _ => {
                panic!("conversion not supported");
            }
        }
    }
}
