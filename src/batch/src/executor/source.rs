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

use risingwave_common::error::Result;

use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct SourceExecutor {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for SourceExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        _source: &ExecutorBuilder<'_, C>,
        _inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        todo!("Can't support SourceExecutor now!")
    }
}
