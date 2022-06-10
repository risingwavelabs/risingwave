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
use risingwave_common::error::{BoxedError, ErrorCode, RwError, TrackingIssue};
use risingwave_expr::ExprError;
use risingwave_storage::error::StorageError;
use thiserror::Error;

use super::Barrier;

#[derive(Error, Debug)]
enum StreamExecutorErrorInner {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[source]
        StorageError,
    ),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Executor v1 error: {0}")]
    ExecutorV1(RwError),

    #[error("Chunk operation error: {0}")]
    EvalError(BoxedError),

    // TODO: remove this after state table is fully used
    #[error("Serialize/deserialize error: {0}")]
    SerdeError(BoxedError),

    #[error("Input error: {0}")]
    InputError(RwError),

    #[error("TopN state error: {0}")]
    TopNStateError(RwError),

    #[error("Hash join error: {0}")]
    HashJoinError(RwError),

    #[error("Source error: {0}")]
    SourceError(RwError),

    #[error("Channel `{0}` closed")]
    ChannelClosed(String),

    #[error("Failed to align barrier: expected {0:?} but got {1:?}")]
    AlignBarrier(Box<Barrier>, Box<Barrier>),

    #[error("Feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),

    #[error(transparent)]
    Internal(anyhow::Error),
}

impl StreamExecutorError {
    pub fn storage(error: impl Into<StorageError>) -> Self {
        StreamExecutorErrorInner::Storage(error.into()).into()
    }

    pub fn executor_v1(error: impl Into<RwError>) -> Self {
        StreamExecutorErrorInner::ExecutorV1(error.into()).into()
    }

    pub fn eval_error(error: impl std::error::Error + Send + Sync) -> Self {
        StreamExecutorErrorInner::EvalError(error.into()).into()
    }

    pub fn serde_error(error: impl std::error::Error + Send + Sync) -> Self {
        StreamExecutorErrorInner::SerdeError(error.into()).into()
    }

    pub fn input_error(error: impl Into<RwError>) -> Self {
        StreamExecutorErrorInner::InputError(error.into()).into()
    }

    pub fn top_n_state_error(error: impl Into<RwError>) -> Self {
        StreamExecutorErrorInner::TopNStateError(error.into()).into()
    }

    pub fn hash_join_error(error: impl Into<RwError>) -> Self {
        StreamExecutorErrorInner::HashJoinError(error.into()).into()
    }

    pub fn source_error(error: impl Into<RwError>) -> Self {
        StreamExecutorErrorInner::SourceError(error.into()).into()
    }

    pub fn channel_closed(name: impl Into<String>) -> Self {
        StreamExecutorErrorInner::ChannelClosed(name.into()).into()
    }

    pub fn align_barrier(expected: Barrier, received: Barrier) -> Self {
        StreamExecutorErrorInner::AlignBarrier(expected.into(), received.into()).into()
    }

    pub fn invalid_argument(error: impl Into<String>) -> Self {
        StreamExecutorErrorInner::InvalidArgument(error.into()).into()
    }

    pub fn not_implemented(error: impl Into<String>, issue: impl Into<TrackingIssue>) -> Self {
        StreamExecutorErrorInner::NotImplemented(error.into(), issue.into()).into()
    }
}

#[derive(Error)]
#[error("{inner}")]
pub struct StreamExecutorError {
    #[from]
    inner: StreamExecutorErrorInner,
    backtrace: Backtrace,
}

impl std::fmt::Debug for StreamExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `StreamExecutorError`:\n{}",
                self.backtrace
            )?;
        }
        Ok(())
    }
}

impl From<StorageError> for StreamExecutorError {
    fn from(s: StorageError) -> Self {
        Self::storage(s)
    }
}

impl From<ArrayError> for StreamExecutorError {
    fn from(a: ArrayError) -> Self {
        Self::eval_error(a)
    }
}

impl From<ExprError> for StreamExecutorError {
    fn from(e: ExprError) -> Self {
        Self::eval_error(e)
    }
}

impl From<anyhow::Error> for StreamExecutorError {
    fn from(a: anyhow::Error) -> Self {
        StreamExecutorErrorInner::Internal(a).into()
    }
}

impl From<memcomparable::Error> for StreamExecutorError {
    fn from(m: memcomparable::Error) -> Self {
        Self::serde_error(m)
    }
}

/// Always convert [`StreamExecutorError`] to stream error variant of [`RwError`].
impl From<StreamExecutorError> for RwError {
    fn from(h: StreamExecutorError) -> Self {
        ErrorCode::StreamError(h.into()).into()
    }
}

pub type StreamExecutorResult<T> = std::result::Result<T, StreamExecutorError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn func_return_error() -> StreamExecutorResult<()> {
        Err(ErrorCode::InternalError("test_error".into())).map_err(StreamExecutorError::executor_v1)
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
