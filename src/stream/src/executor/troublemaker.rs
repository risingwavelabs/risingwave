// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::Op;

use super::{BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};

/// TroublemakerExecutor is used to make some trouble in the stream graph. Specifically,
/// it is attached to `StreamScan` and `Source` executors in **insane mode**. It randomly
/// corrupts the stream chunks it receives and sends them downstream, making the stream
/// inconsistent. This should ONLY BE USED IN INSANE MODE FOR TESTING PURPOSES.
pub struct TroublemakerExecutor {
    input: Executor,
}

struct ExecutionVars {
    // TODO()
}

impl TroublemakerExecutor {
    pub fn new(input: Executor) -> Self {
        assert!(
            crate::consistency::insane(),
            "we should only make trouble in insane mode"
        );
        Self { input }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let mut vars = ExecutionVars {};

        #[for_await]
        for msg in self.input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    // TODO(): make some trouble
                    yield Message::Chunk(chunk);
                }
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
                _ => yield msg,
            }
        }
    }
}

impl Execute for TroublemakerExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
