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

use futures::StreamExt;
use risingwave_common::catalog::Schema;

use crate::executor::{BoxedMessageStream, Executor, ExecutorInfo, PkIndicesRef};

#[derive(Default)]
pub struct DummyExecutor {
    pub info: ExecutorInfo,
}

impl DummyExecutor {
    pub fn new() -> Self {
        Self {
            info: ExecutorInfo {
                schema: Schema::empty().clone(),
                pk_indices: vec![],
                identity: "DummyExecutor".to_string(),
            },
        }
    }
}

impl Executor for DummyExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        futures::stream::pending().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
