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

use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

#[derive(Error, Debug)]
enum StorageTableErrorInner {
    #[error("TableIteratorError {0}")]
    TableIteratorError(String),

    #[error("State store iterator error {0}")]
    StateStoreIteratorError(String),

    #[error("Wait epoch error {0}")]
    WaitEpochError(String),

    #[error("Deserialize row error {0}.")]
    DeserializeRowError(String),

    #[error("state store get error {0}.")]
    StateStoreGetError(String),
}

#[derive(Error)]
#[error("{inner}")]
pub struct StorageTableError {
    #[from]
    inner: StorageTableErrorInner,
    backtrace: Backtrace,
}

impl StorageTableError {
    pub fn table_iterator_error(error: impl ToString) -> StorageTableError {
        StorageTableErrorInner::TableIteratorError(error.to_string()).into()
    }

    pub fn state_store_iterator_error(error: impl ToString) -> StorageTableError {
        StorageTableErrorInner::StateStoreIteratorError(error.to_string()).into()
    }

    pub fn wait_epoch_error(error: impl ToString) -> StorageTableError {
        StorageTableErrorInner::WaitEpochError(error.to_string()).into()
    }

    pub fn deserialize_row_error(error: impl ToString) -> StorageTableError {
        StorageTableErrorInner::DeserializeRowError(error.to_string()).into()
    }

    pub fn state_store_get_error(error: impl ToString) -> StorageTableError {
        StorageTableErrorInner::StateStoreGetError(error.to_string()).into()
    }
}

impl std::fmt::Debug for StorageTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `TracedStorageTableError`:\n{}",
                self.backtrace
            )?;
        }
        Ok(())
    }
}

impl From<StorageTableError> for RwError {
    fn from(s: StorageTableError) -> Self {
        ErrorCode::StorageTableError(Box::new(s).to_string()).into()
    }
}

pub type StorageTableResult<T> = std::result::Result<T, StorageTableError>;

#[derive(Error, Debug)]
enum StateTableErrorInner {
    #[error("state store get error {0}.")]
    StateStoreGetError(String),

    #[error("Deserialize row error {0}.")]
    DeserializeRowError(String),

    #[error("Invalid Batch write rows {0}.")]
    InvalidBatchWriteRows(String),

    #[error("State table row iterator error {0}.")]
    StateTableIteratorError(String),

    #[error("State store iterator error {0}.")]
    StateStoreIteratorError(String),
}

#[derive(Error)]
#[error("{inner}")]
pub struct StateTableError {
    #[from]
    inner: StateTableErrorInner,
    backtrace: Backtrace,
}

impl StateTableError {
    pub fn state_store_get_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::StateStoreGetError(error.to_string()).into()
    }

    pub fn deserialize_row_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::DeserializeRowError(error.to_string()).into()
    }

    pub fn batch_write_rows_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::InvalidBatchWriteRows(error.to_string()).into()
    }

    pub fn state_table_row_iterator_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::StateTableIteratorError(error.to_string()).into()
    }

    pub fn state_store_iterator_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::StateStoreIteratorError(error.to_string()).into()
    }
}

impl std::fmt::Debug for StateTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `TracedStateTableError`:\n{}",
                self.backtrace
            )?;
        }
        Ok(())
    }
}

impl From<StateTableError> for RwError {
    fn from(s: StateTableError) -> Self {
        ErrorCode::StateTableError(Box::new(s).to_string()).into()
    }
}
pub type StateTableResult<T> = std::result::Result<T, StateTableError>;
