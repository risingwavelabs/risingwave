// Copyright 2026 RisingWave Labs
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

use std::error::Error;

/// An error building, decoding, searching, or merging full-text index data.
#[derive(Debug, thiserror::Error)]
pub enum FullTextError {
    #[error("Magic number mismatch: expected {expected}, found: {found}")]
    MagicMismatch { expected: u32, found: u32 },
    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u32),
    #[error("Checksum mismatch: expected {expected}, found: {found}")]
    ChecksumMismatch { expected: u64, found: u64 },
    #[error("Decode error: {0}")]
    Decode(String),
    #[error("Full-text index error: {0}")]
    Other(String),
    #[error("full-text index")]
    Backend(#[source] Box<dyn Error + Send + Sync>),
}

impl FullTextError {
    pub(crate) fn decode(message: impl Into<String>) -> Self {
        Self::Decode(message.into())
    }

    pub(crate) fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }
}

pub type FullTextResult<T> = std::result::Result<T, FullTextError>;

pub fn full_text_error(error: impl Error + Send + Sync + 'static) -> FullTextError {
    FullTextError::Backend(Box::new(error))
}
