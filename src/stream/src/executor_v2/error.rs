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

#[derive(Error, Debug)]
pub enum StreamExecutorError {
    #[error("storage error {0}")]
    Storage(
        #[backtrace]
        #[source]
        StorageError,
    ),

    #[error("executor v1 error {0}")]
    ExecutorV1(RwError),

    #[error("chunk operation error {0}")]
    EvalError(RwError),

    #[error("aggregate state error {0}")]
    AggStateError(RwError),

    #[error("channel `{0}` closed")]
    ChannelClosed(String),
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

    pub fn channel_closed(name: impl Into<String>) -> TracedStreamExecutorError {
        Self::ChannelClosed(name.into()).into()
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

/// Always convert [`TracedStreamExecutorError`] to internal error of `RwResult`.
impl From<TracedStreamExecutorError> for RwError {
    fn from(h: TracedStreamExecutorError) -> Self {
        ErrorCode::InternalError(h.to_string()).into()
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
