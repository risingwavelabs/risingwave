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

use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use thiserror::Error;

use crate::hummock::HummockError;
use crate::mem_table::MemTableError;

#[derive(Error, Debug, thiserror_ext::Box)]
#[thiserror_ext(newtype(name = StorageError, backtrace, report_debug))]
pub enum ErrorKind {
    #[error("Hummock error: {0}")]
    Hummock(
        #[backtrace]
        #[from]
        HummockError,
    ),

    #[error("Deserialize row error {0}.")]
    DeserializeRow(
        #[from]
        #[backtrace]
        ValueEncodingError,
    ),

    #[error("Serialize/deserialize error: {0}")]
    SerdeError(
        #[from]
        #[backtrace]
        memcomparable::Error,
    ),

    #[error("Sled error: {0}")]
    Sled(
        #[backtrace]
        #[from]
        sled::Error,
    ),

    #[error("MemTable error: {0}")]
    MemTable(
        #[backtrace]
        #[from]
        Box<MemTableError>,
    ),
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

impl From<StorageError> for RwError {
    fn from(s: StorageError) -> Self {
        ErrorCode::StorageError(Box::new(s)).into()
    }
}
