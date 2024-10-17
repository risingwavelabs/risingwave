// Copyright 2024 RisingWave Labs
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

mod transaction;

pub type ColumnFamily = String;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

use thiserror::Error;
pub use transaction::*;

// Error of metastore
#[derive(Debug, Error)]
pub enum MetaStoreError {
    #[error("item not found: {0}")]
    ItemNotFound(String),
    #[error("transaction abort")]
    TransactionAbort(),
    #[error("internal error: {0}")]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

pub type MetaStoreResult<T> = std::result::Result<T, MetaStoreError>;
