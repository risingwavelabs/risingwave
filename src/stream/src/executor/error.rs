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

use risingwave_common::array::ArrayError;
use risingwave_common::error::{BoxedError, NotImplemented};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_dml::error::DmlError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::RpcError;
use risingwave_storage::error::StorageError;
use strum_macros::AsRefStr;

use super::Barrier;
use super::exchange::error::ExchangeChannelClosed;

/// A specialized Result type for streaming executors.
pub type StreamExecutorResult<T> = std::result::Result<T, StreamExecutorError>;

/// The error type for streaming executors.
#[derive(
    thiserror::Error, thiserror_ext::ReportDebug, thiserror_ext::Box, thiserror_ext::Construct,
)]
#[thiserror_ext(newtype(name = StreamExecutorError, backtrace))]
#[derive(AsRefStr)]
pub enum ErrorKind {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[from]
        StorageError,
    ),

    #[error("Chunk operation error: {0}")]
    ArrayError(
        #[from]
        #[backtrace]
        ArrayError,
    ),

    #[error("Chunk operation error: {0}")]
    ExprError(
        #[from]
        #[backtrace]
        ExprError,
    ),

    // TODO: remove this after state table is fully used
    #[error("Serialize/deserialize error: {0}")]
    SerdeError(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Sink error: sink_id={1}, error: {0}")]
    SinkError(
        #[source]
        #[backtrace]
        SinkError,
        u32,
    ),

    #[error(transparent)]
    RpcError(
        #[from]
        #[backtrace]
        RpcError,
    ),

    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    #[error(transparent)]
    ExchangeChannelClosed(
        #[from]
        #[backtrace]
        ExchangeChannelClosed,
    ),

    #[error("Failed to align barrier: expected `{0:?}` but got `{1:?}`")]
    AlignBarrier(Box<Barrier>, Box<Barrier>),

    #[error("Connector error: {0}")]
    ConnectorError(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error(transparent)]
    DmlError(
        #[from]
        #[backtrace]
        DmlError,
    ),

    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),

    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
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

/// Connector error.
impl From<ConnectorError> for StreamExecutorError {
    fn from(s: ConnectorError) -> Self {
        Self::connector_error(s)
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

impl From<String> for StreamExecutorError {
    fn from(s: String) -> Self {
        ErrorKind::Uncategorized(anyhow::anyhow!(s)).into()
    }
}

impl From<(SinkError, u32)> for StreamExecutorError {
    fn from((err, sink_id): (SinkError, u32)) -> Self {
        ErrorKind::SinkError(err, sink_id).into()
    }
}

impl StreamExecutorError {
    pub fn variant_name(&self) -> &str {
        self.0.inner().as_ref()
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
