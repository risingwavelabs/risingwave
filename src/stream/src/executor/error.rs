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
use risingwave_common::error::{BoxedError, Error, TrackingIssue};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::RpcError;
use risingwave_storage::error::StorageError;

use super::Barrier;
use crate::common::log_store::LogStoreError;

/// A specialized Result type for streaming executors.
pub type StreamExecutorResult<T> = std::result::Result<T, StreamExecutorError>;

/// The error type for streaming executors.
#[derive(thiserror::Error)]
#[error("{inner}")]
pub struct StreamExecutorError {
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

    #[error("Log store error: {0}")]
    LogStoreError(#[source] LogStoreError),

    #[error("Chunk operation error: {0}")]
    ArrayError(#[source] ArrayError),

    #[error("Chunk operation error: {0}")]
    ExprError(#[source] ExprError),

    // TODO: remove this after state table is fully used
    #[error("Serialize/deserialize error: {0}")]
    SerdeError(#[source] BoxedError),

    #[error("Sink error: {0}")]
    SinkError(#[source] SinkError),

    #[error("RPC error: {0}")]
    RpcError(#[source] RpcError),

    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    #[error("Failed to align barrier: expected {0:?} but got {1:?}")]
    AlignBarrier(Box<Barrier>, Box<Barrier>),

    #[error("Connector error: {0}")]
    ConnectorError(#[source] BoxedError),

    #[error("Dml error: {0}")]
    DmlError(#[source] BoxedError),

    #[error("Feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),

    #[error(transparent)]
    Internal(anyhow::Error),
}

impl StreamExecutorError {
    fn serde_error(error: impl Error) -> Self {
        ErrorKind::SerdeError(error.into()).into()
    }

    pub fn channel_closed(name: impl Into<String>) -> Self {
        ErrorKind::ChannelClosed(name.into()).into()
    }

    pub fn align_barrier(expected: Barrier, received: Barrier) -> Self {
        ErrorKind::AlignBarrier(expected.into(), received.into()).into()
    }

    pub fn connector_error(error: impl Error) -> Self {
        ErrorKind::ConnectorError(error.into()).into()
    }

    pub fn not_implemented(error: impl Into<String>, issue: impl Into<TrackingIssue>) -> Self {
        ErrorKind::NotImplemented(error.into(), issue.into()).into()
    }

    pub fn dml_error(error: impl Error) -> Self {
        ErrorKind::DmlError(error.into()).into()
    }
}

impl std::fmt::Debug for StreamExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner.kind)?;
        writeln!(f)?;
        if let Some(backtrace) = (&self.inner.kind as &dyn Error).request_ref::<Backtrace>() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `StreamExecutorError`:\n{}",
                self.inner.backtrace
            )?;
        }
        Ok(())
    }
}

impl From<ErrorKind> for StreamExecutorError {
    fn from(kind: ErrorKind) -> Self {
        Self {
            inner: Box::new(kind.into()),
        }
    }
}

/// Storage error.
impl From<StorageError> for StreamExecutorError {
    fn from(s: StorageError) -> Self {
        ErrorKind::Storage(s).into()
    }
}

/// Log store error
impl From<LogStoreError> for StreamExecutorError {
    fn from(e: LogStoreError) -> Self {
        ErrorKind::LogStoreError(e).into()
    }
}

/// Chunk operation error.
impl From<ArrayError> for StreamExecutorError {
    fn from(e: ArrayError) -> Self {
        ErrorKind::ArrayError(e).into()
    }
}

impl From<ExprError> for StreamExecutorError {
    fn from(e: ExprError) -> Self {
        ErrorKind::ExprError(e).into()
    }
}

/// Internal error.
impl From<anyhow::Error> for StreamExecutorError {
    fn from(a: anyhow::Error) -> Self {
        ErrorKind::Internal(a).into()
    }
}

/// Serialize/deserialize error.
impl From<memcomparable::Error> for StreamExecutorError {
    fn from(m: memcomparable::Error) -> Self {
        Self::serde_error(m)
    }
}
impl From<ValueEncodingError> for StreamExecutorError {
    fn from(e: ValueEncodingError) -> Self {
        Self::serde_error(e)
    }
}

impl From<RpcError> for StreamExecutorError {
    fn from(e: RpcError) -> Self {
        ErrorKind::RpcError(e).into()
    }
}

/// Connector error.
impl From<ConnectorError> for StreamExecutorError {
    fn from(s: ConnectorError) -> Self {
        Self::connector_error(s)
    }
}

impl From<SinkError> for StreamExecutorError {
    fn from(e: SinkError) -> Self {
        ErrorKind::SinkError(e).into()
    }
}

impl From<PbFieldNotFound> for StreamExecutorError {
    fn from(err: PbFieldNotFound) -> Self {
        Self::from(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
    }
}

static_assertions::const_assert_eq!(std::mem::size_of::<StreamExecutorError>(), 8);

#[cfg(test)]
mod tests {
    use risingwave_common::bail;

    use super::*;

    fn func_return_error() -> StreamExecutorResult<()> {
        bail!("test_error")
    }

    #[test]
    #[should_panic]
    #[ignore]
    fn executor_error_ui_test_1() {
        // For this test, ensure that we have only one backtrace from error when panic.
        func_return_error().unwrap();
    }

    #[test]
    #[ignore]
    fn executor_error_ui_test_2() {
        // For this test, ensure that we have only one backtrace from error when panic.
        func_return_error().map_err(|e| println!("{:?}", e)).ok();
    }
}
