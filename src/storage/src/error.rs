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

use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use thiserror::Error;

use crate::hummock::HummockError;
use crate::table::error::StateTableError;

#[derive(Error)]
pub enum StorageError {
    #[error("Hummock error: {0}")]
    Hummock(
        #[backtrace]
        #[from]
        HummockError,
    ),

    #[error("Deserialize row error {0}.")]
    DeserializeRow(ValueEncodingError),

    #[error("State table error: {0}")]
    StateTable(
        #[backtrace]
        #[source]
        #[from]
        StateTableError,
    ),
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

impl From<ValueEncodingError> for StorageError {
    fn from(error: ValueEncodingError) -> Self {
        StorageError::DeserializeRow(error)
    }
}

impl From<StorageError> for RwError {
    fn from(s: StorageError) -> Self {
        ErrorCode::StorageError(Box::new(s)).into()
    }
}

impl std::fmt::Debug for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self)?;
        writeln!(f)?;
        if let Some(backtrace) = self.backtrace() {
            // Since we forward all backtraces from source, `self.backtrace()` is the backtrace of
            // inner error.
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        }

        Ok(())
    }
}
