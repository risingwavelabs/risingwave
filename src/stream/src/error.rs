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

use risingwave_common::array::ArrayError;
use risingwave_expr::ExprError;
use risingwave_pb::ProstFieldNotFound;
use risingwave_storage::error::StorageError;

use crate::executor::StreamExecutorError;

#[derive(thiserror::Error, Debug)]
enum Inner {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[source]
        StorageError,
    ),

    #[error("Expression error: {0}")]
    Expression(ExprError),

    #[error("Array/Chunk error: {0}")]
    Array(ArrayError),

    #[error("Executor error: {0}")]
    Executor(Box<StreamExecutorError>),

    #[error(transparent)]
    Internal(anyhow::Error),
}

/// Error type for streaming tasks.
#[derive(thiserror::Error)]
#[error("{inner}")]
pub struct StreamError {
    #[from]
    inner: Inner,
    backtrace: Backtrace,
}

impl std::fmt::Debug for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(f, "  backtrace of `StreamError`:\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

// Storage transaction error; ...
impl From<StorageError> for StreamError {
    fn from(s: StorageError) -> Self {
        Inner::Storage(s).into()
    }
}

// Build expression error; ...
impl From<ExprError> for StreamError {
    fn from(error: ExprError) -> Self {
        Inner::Expression(error).into()
    }
}

// Chunk compaction error; ProtoBuf ser/de error; ...
impl From<ArrayError> for StreamError {
    fn from(error: ArrayError) -> Self {
        Inner::Array(error).into()
    }
}

// Executor runtime error; ...
impl From<StreamExecutorError> for StreamError {
    fn from(error: StreamExecutorError) -> Self {
        Inner::Executor(Box::new(error)).into()
    }
}

impl From<ProstFieldNotFound> for StreamError {
    fn from(err: ProstFieldNotFound) -> Self {
        Self::from(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
    }
}

// Internal error.
impl From<anyhow::Error> for StreamError {
    fn from(a: anyhow::Error) -> Self {
        Inner::Internal(a).into()
    }
}

impl From<StreamError> for tonic::Status {
    fn from(error: StreamError) -> Self {
        // Only encode the error message without the backtrace.
        tonic::Status::internal(error.inner.to_string())
    }
}

pub type StreamResult<T> = std::result::Result<T, StreamError>;
