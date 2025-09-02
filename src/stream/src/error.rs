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
use risingwave_common::error::tonic::extra::{Score, ScoredError};
use risingwave_common::secret::SecretError;
use risingwave_connector::error::ConnectorError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::ToTonicStatus;
use risingwave_storage::error::StorageError;

use crate::executor::exchange::error::ExchangeChannelClosed;
use crate::executor::{Barrier, StreamExecutorError};
use crate::task::ActorId;

/// A specialized Result type for streaming tasks.
pub type StreamResult<T> = std::result::Result<T, StreamError>;

/// The error type for streaming tasks.
#[derive(
    thiserror::Error,
    thiserror_ext::ReportDebug,
    thiserror_ext::Arc,
    thiserror_ext::ContextInto,
    thiserror_ext::Construct,
)]
#[thiserror_ext(newtype(name = StreamError, backtrace))]
pub enum ErrorKind {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[from]
        StorageError,
    ),

    #[error("Expression error: {0}")]
    Expression(
        #[from]
        #[backtrace]
        ExprError,
    ),

    #[error("Array/Chunk error: {0}")]
    Array(
        #[from]
        #[backtrace]
        ArrayError,
    ),

    #[error("Executor error: {0}")]
    Executor(
        #[from]
        #[backtrace]
        StreamExecutorError,
    ),

    #[error("Actor {actor_id} exited unexpectedly: {source}")]
    UnexpectedExit {
        actor_id: ActorId,
        #[backtrace]
        source: StreamError,
    },

    #[error("Failed to send barrier with epoch {epoch} to actor {actor_id}: {reason}", epoch = .barrier.epoch.curr)]
    BarrierSend {
        barrier: Barrier,
        actor_id: ActorId,
        reason: &'static str,
    },

    #[error("Secret error: {0}")]
    Secret(
        #[from]
        #[backtrace]
        SecretError,
    ),

    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
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

impl From<ExchangeChannelClosed> for StreamError {
    fn from(err: ExchangeChannelClosed) -> Self {
        StreamExecutorError::from(err).into()
    }
}

impl From<StreamError> for tonic::Status {
    fn from(error: StreamError) -> Self {
        error.to_status(tonic::Code::Internal, "stream")
    }
}

static_assertions::const_assert_eq!(std::mem::size_of::<StreamError>(), 8);

/// A [`StreamError`] with a score, used to find the root cause of actor failures.
pub type ScoredStreamError = ScoredError<StreamError>;

impl StreamError {
    /// Score the given error based on hard-coded rules.
    pub fn with_score(self) -> ScoredStreamError {
        // Explicitly list all error kinds here to notice developers to update this function when
        // there are changes in error kinds.

        fn stream_executor_error_score(e: &StreamExecutorError) -> i32 {
            use crate::executor::error::ErrorKind;
            match e.inner() {
                // `ChannelClosed` or `ExchangeChannelClosed` is likely to be caused by actor exit
                // and not the root cause.
                ErrorKind::ChannelClosed(_) | ErrorKind::ExchangeChannelClosed(_) => 1,

                // Normal errors.
                ErrorKind::Uncategorized(_)
                | ErrorKind::Storage(_)
                | ErrorKind::ArrayError(_)
                | ErrorKind::ExprError(_)
                | ErrorKind::SerdeError(_)
                | ErrorKind::SinkError(_, _)
                | ErrorKind::RpcError(_)
                | ErrorKind::AlignBarrier(_, _)
                | ErrorKind::ConnectorError(_)
                | ErrorKind::DmlError(_)
                | ErrorKind::NotImplemented(_) => 999,
            }
        }

        fn stream_error_score(e: &StreamError) -> i32 {
            use crate::error::ErrorKind;
            match e.inner() {
                // `UnexpectedExit` wraps the original error. Score on the inner error.
                ErrorKind::UnexpectedExit { source, .. } => stream_error_score(source),

                // `BarrierSend` is likely to be caused by actor exit and not the root cause.
                ErrorKind::BarrierSend { .. } => 1,

                // Executor errors first.
                ErrorKind::Executor(ee) => 2000 + stream_executor_error_score(ee),

                // Then other errors.
                ErrorKind::Uncategorized(_)
                | ErrorKind::Storage(_)
                | ErrorKind::Expression(_)
                | ErrorKind::Array(_)
                | ErrorKind::Secret(_) => 1000,
            }
        }

        let score = Score(stream_error_score(&self));
        ScoredStreamError { error: self, score }
    }
}
