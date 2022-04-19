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

use std::mem::swap;

use futures::stream::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;

use crate::executor::executor2_wrapper::WrapperState::{Closed, Created, Open};
use crate::executor::Executor;
use crate::executor2::{BoxedDataChunkStream, BoxedExecutor2, ExecutorInfo};

enum WrapperState {
    Created { executor: BoxedExecutor2 },
    Open { stream: BoxedDataChunkStream },
    Closed,
}

/// Wrap an `Executor2` instance to convert it into an `Executor`.
pub struct Executor2Wrapper {
    info: ExecutorInfo,
    state: WrapperState,
}

#[async_trait::async_trait]
impl Executor for Executor2Wrapper {
    async fn open(&mut self) -> Result<()> {
        let mut tmp = WrapperState::Closed;
        swap(&mut tmp, &mut self.state);
        match tmp {
            Created { executor } => {
                self.state = Open {
                    stream: executor.execute(),
                };
                Ok(())
            }
            _ => Err(InternalError("Executor already open!".to_string()).into()),
        }
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        match &mut self.state {
            Open { ref mut stream } => stream.next().await.transpose(),
            _ => Err(InternalError("Executor not in open state!".to_string()).into()),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.state = Closed;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn identity(&self) -> &str {
        self.info.id.as_str()
    }
}

impl From<BoxedExecutor2> for Executor2Wrapper {
    fn from(executor2: BoxedExecutor2) -> Self {
        let executor_info = ExecutorInfo {
            schema: executor2.schema().to_owned(),
            id: executor2.identity().to_string(),
        };

        Self {
            info: executor_info,
            state: WrapperState::Created {
                executor: executor2,
            },
        }
    }
}
