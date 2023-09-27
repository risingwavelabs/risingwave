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

use std::backtrace::Backtrace;

use risingwave_common::array::ArrayError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_storage::error::StorageError;

use crate::executor::StreamExecutorError;

/// A specialized Result type for streaming tasks.
pub type StreamResult<T> = std::result::Result<T, StreamError>;

/// The error type for streaming tasks.
#[derive(thiserror::Error)]
#[error("{inner}")]
pub struct StreamError {
    inner: Box<Inner>,
}

#[derive(thiserror::Error, Debug)]
#[error("{kind}")]
struct Inner {
    #[from]
    kind: ErrorKind,
    backtrace: Backtrace,
}

#[derive(thiserror::Error, Debug)]
enum ErrorKind {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[source]
        StorageError,
    ),

    #[error("Expression error: {0}")]
    Expression(#[source] ExprError),

    #[error("Array/Chunk error: {0}")]
    Array(#[source] ArrayError),

    #[error("Executor error: {0:?}")]
    Executor(#[source] StreamExecutorError),

    #[error("Sink error: {0:?}")]
    Sink(#[source] SinkError),

    #[error(transparent)]
    Internal(anyhow::Error),
}

impl std::fmt::Debug for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner.kind)?;
        writeln!(f)?;
        if let Some(backtrace) =
            std::error::request_ref::<Backtrace>(&self.inner.kind as &dyn Error)
        {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(f, "  backtrace of `StreamError`:\n{}", self.inner.backtrace)?;
        }
        Ok(())
    }
}

impl From<ErrorKind> for StreamError {
    fn from(kind: ErrorKind) -> Self {
        Self {
            inner: Box::new(kind.into()),
        }
    }
}

// Storage transaction error; ...
impl From<StorageError> for StreamError {
    fn from(s: StorageError) -> Self {
        ErrorKind::Storage(s).into()
    }
}

// Build expression error; ...
impl From<ExprError> for StreamError {
    fn from(error: ExprError) -> Self {
        ErrorKind::Expression(error).into()
    }
}

// Chunk compaction error; ProtoBuf ser/de error; ...
impl From<ArrayError> for StreamError {
    fn from(error: ArrayError) -> Self {
        ErrorKind::Array(error).into()
    }
}

// Executor runtime error; ...
impl From<StreamExecutorError> for StreamError {
    fn from(error: StreamExecutorError) -> Self {
        ErrorKind::Executor(error).into()
    }
}

impl From<SinkError> for StreamError {
    fn from(value: SinkError) -> Self {
        ErrorKind::Sink(value).into()
    }
}

impl From<PbFieldNotFound> for StreamError {
    fn from(err: PbFieldNotFound) -> Self {
        Self::from(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
    }
}

impl From<ConnectorError> for StreamError {
    fn from(err: ConnectorError) -> Self {
        StreamExecutorError::from(err).into()
    }
}

// Internal error.
impl From<anyhow::Error> for StreamError {
    fn from(a: anyhow::Error) -> Self {
        ErrorKind::Internal(a).into()
    }
}

impl From<StreamError> for tonic::Status {
    fn from(error: StreamError) -> Self {
        // Only encode the error message without the backtrace.
        tonic::Status::internal(error.inner.to_string())
    }
}

static_assertions::const_assert_eq!(std::mem::size_of::<StreamError>(), 8);
