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
enum StateTableErrorInner {
    #[error("state table get row error {0}.")]
    StateTableGetRowError(String),
    #[error("Serialize row error {0}.")]
    SerializeRowError(String),
    #[error("Deserialize row error {0}.")]
    DeserializeRowError(String),
    #[error("Batch write rows error {0}.")]
    BatchWriteRowsError(String),
    #[error("Iterator error {0}.")]
    IteratorError(String),
}

#[derive(Error)]
#[error("{inner}")]
pub struct StateTableError {
    #[from]
    inner: StateTableErrorInner,
    backtrace: Backtrace,
}

impl StateTableError {
    pub fn state_table_point_get_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::StateTableGetRowError(error.to_string()).into()
    }

    pub fn serialize_row_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::SerializeRowError(error.to_string()).into()
    }

    pub fn deserialize_row_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::DeserializeRowError(error.to_string()).into()
    }

    pub fn batch_write_rows_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::BatchWriteRowsError(error.to_string()).into()
    }

    pub fn iterator_error(error: impl ToString) -> StateTableError {
        StateTableErrorInner::IteratorError(error.to_string()).into()
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
