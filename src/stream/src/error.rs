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

use risingwave_common::array::ArrayError;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::SinkError;
use risingwave_expr::ExprError;
use risingwave_pb::PbFieldNotFound;
use risingwave_rpc_client::error::ToTonicStatus;
use risingwave_storage::error::StorageError;

use crate::executor::{Barrier, StreamExecutorError};
use crate::task::ActorId;

/// A specialized Result type for streaming tasks.
pub type StreamResult<T> = std::result::Result<T, StreamError>;

/// The error type for streaming tasks.
#[derive(
    thiserror::Error,
    Debug,
    thiserror_ext::Arc,
    thiserror_ext::ContextInto,
    thiserror_ext::Construct,
)]
#[thiserror_ext(newtype(name = StreamError, backtrace, report_debug))]
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

    #[error("Sink error: {0}")]
    Sink(
        #[from]
        #[backtrace]
        SinkError,
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

    #[error(transparent)]
    Internal(
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

impl From<StreamError> for tonic::Status {
    fn from(error: StreamError) -> Self {
        error.to_status(tonic::Code::Internal, "stream")
    }
}

static_assertions::const_assert_eq!(std::mem::size_of::<StreamError>(), 8);
