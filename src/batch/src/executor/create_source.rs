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
use risingwave_common::error::Result;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::executor::Executor;

// TODO: Remove this when Java frontend is completely removed.
pub(super) struct CreateSourceExecutor {}

impl BoxedExecutorBuilder for CreateSourceExecutor {
    fn new_boxed_executor(_: &ExecutorBuilder) -> Result<BoxedExecutor> {
        unreachable!()
    }
}

#[async_trait::async_trait]
impl Executor for CreateSourceExecutor {
    async fn open(&mut self) -> Result<()> {
        unreachable!()
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        unreachable!()
    }

    async fn close(&mut self) -> Result<()> {
        unreachable!()
    }

    fn schema(&self) -> &Schema {
        unreachable!()
    }

    fn identity(&self) -> &str {
        unreachable!()
    }
}
