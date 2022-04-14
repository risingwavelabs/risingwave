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

use risingwave_common::error::{ErrorCode, RwError};
use risingwave_storage::error::StorageError;
use thiserror::Error;

use super::Barrier;

#[derive(Error, Debug)]
pub enum StreamExecutorError {
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
    EvalError(RwError),

    #[error("Aggregate state error: {0}")]
    AggStateError(RwError),

    #[error("Input error: {0}")]
    InputError(RwError),

    #[error("TopN state error: {0}")]
    TopNStateError(RwError),

    #[error("Channel `{0}` closed")]
    ChannelClosed(String),

    #[error("Failed to align barrier: expected {0:?} but got {1:?}")]
    AlignBarrier(Box<Barrier>, Box<Barrier>),
}

impl StreamExecutorError {
    pub fn storage(error: impl Into<StorageError>) -> TracedStreamExecutorError {
        Self::Storage(error.into()).into()
    }

    pub fn executor_v1(error: impl Into<RwError>) -> TracedStreamExecutorError {
        Self::ExecutorV1(error.into()).into()
    }

    pub fn eval_error(error: impl Into<RwError>) -> TracedStreamExecutorError {
        Self::EvalError(error.into()).into()
    }

    pub fn agg_state_error(error: impl Into<RwError>) -> TracedStreamExecutorError {
        Self::AggStateError(error.into()).into()
    }

    pub fn input_error(error: impl Into<RwError>) -> TracedStreamExecutorError {
        Self::InputError(error.into()).into()
    }

    pub fn top_n_state_error(error: impl Into<RwError>) -> TracedStreamExecutorError {
        Self::TopNStateError(error.into()).into()
    }

    pub fn channel_closed(name: impl Into<String>) -> TracedStreamExecutorError {
        Self::ChannelClosed(name.into()).into()
    }

    pub fn align_barrier(expected: Barrier, received: Barrier) -> TracedStreamExecutorError {
        Self::AlignBarrier(expected.into(), received.into()).into()
    }
}

#[derive(Error)]
#[error("{source}")]
pub struct TracedStreamExecutorError {
    #[from]
    source: StreamExecutorError,
    backtrace: Backtrace,
}

impl std::fmt::Debug for TracedStreamExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.source)?;
        writeln!(f)?;
        if let Some(backtrace) = self.source.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `TracedStreamExecutorError`:\n{}",
                self.backtrace
            )?;
        }
        Ok(())
    }
}

impl From<StorageError> for TracedStreamExecutorError {
    fn from(s: StorageError) -> Self {
        StreamExecutorError::storage(s)
    }
}

/// Always convert [`TracedStreamExecutorError`] to stream error of `RwResult`.
impl From<TracedStreamExecutorError> for RwError {
    fn from(h: TracedStreamExecutorError) -> Self {
        ErrorCode::StreamError(h.into()).into()
    }
}

pub type StreamExecutorResult<T> = std::result::Result<T, TracedStreamExecutorError>;

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
