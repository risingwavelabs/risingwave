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

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::executor::Executor;

/// [`FuseExecutor`] is a wrapper around a Executor. After wrapping, once a call to
/// `next` returns `Ok(None)`, all subsequent calls to `next` will return an
/// error.
pub struct FusedExecutor<T: BoxedExecutorBuilder + Executor> {
    /// The underlying executor.
    inner: T,
    /// Whether the underlying executor should return `Err` or not.
    invalid: bool,
}

impl<T: BoxedExecutorBuilder + Executor> FusedExecutor<T> {
    pub fn new(executor: T) -> FusedExecutor<T> {
        FusedExecutor {
            inner: executor,
            invalid: false,
        }
    }
}

#[async_trait::async_trait]
impl<T: BoxedExecutorBuilder + Executor> Executor for FusedExecutor<T> {
    async fn open(&mut self) -> Result<()> {
        self.inner.open().await
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.invalid {
            // The executor is invalid now, so we simply return an error.
            return Err(InternalError("Polling an already finished executor".to_string()).into());
        }
        match self.inner.next().await? {
            res @ Some(_) => Ok(res),
            None => {
                // Once the underlying executor returns `Ok(None)`,
                // subsequence calls will return `Err`.
                self.invalid = true;
                Ok(None)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn identity(&self) -> &str {
        self.inner.identity()
    }
}

impl<T: BoxedExecutorBuilder + Executor> BoxedExecutorBuilder for FusedExecutor<T> {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        T::new_boxed_executor(source)
    }
}
