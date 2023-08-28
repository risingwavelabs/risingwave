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

use std::fmt::{Debug, Formatter};




use risingwave_common::catalog::{Schema};





use super::*;

/// `FlowControlExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `FlowControlExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct FlowControlExecutor {
    input: BoxedExecutor,
    rate_limit: Option<usize>,
}

impl FlowControlExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(input: Box<dyn Executor>, rate_limit: Option<usize>) -> Self {
        Self { input, rate_limit }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        #[for_await]
        for msg in self.input.execute() {
            let msg = msg?;
            {
                yield msg;
            }
        }
    }
}

impl Debug for FlowControlExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowControlExecutor")
            .field("rate_limit", &self.rate_limit)
            .finish()
    }
}

impl Executor for FlowControlExecutor {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        "FlowControlExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
