// Copyright 2025 RisingWave Labs
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

/// The error type for DML operations.
#[derive(thiserror::Error, Debug)]
pub enum DmlError {
    #[error("table schema has changed, please try again later")]
    SchemaChanged,

    #[error(
        "DML is not permitted during cluster recovery (no available table reader in streaming executors)"
    )]
    NoReader,

    #[error("table reader closed")]
    ReaderClosed,
}

pub type Result<T> = std::result::Result<T, DmlError>;
