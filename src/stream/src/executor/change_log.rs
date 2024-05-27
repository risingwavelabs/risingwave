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

use core::fmt::Formatter;
use std::fmt::Debug;
use std::sync::Arc;

use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{ArrayImpl, I16Array, Op, StreamChunk};

use super::{ActorContextRef, BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};

pub struct ChangeLogExecutor {
    _ctx: ActorContextRef,
    input: Executor,
}

impl Debug for ChangeLogExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeLogExecutor").finish()
    }
}

impl Execute for ChangeLogExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
impl ChangeLogExecutor {
    pub fn new(ctx: ActorContextRef, input: Executor) -> Self {
        Self { _ctx: ctx, input }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let input = self.input.execute();
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let (ops, columns, bitmap) = chunk.into_inner();
                    let new_ops = vec![Op::Insert; ops.len()];
                    let ops: Vec<Option<i16>> =
                        ops.into_iter().map(|op| Some(op.to_i16())).collect();
                    let ops_array = Arc::new(ArrayImpl::Int16(I16Array::from_iter(ops)));
                    let mut new_columns = Vec::with_capacity(columns.len() + 1);
                    new_columns.extend_from_slice(&columns);
                    new_columns.push(ops_array);
                    let new_chunk = StreamChunk::with_visibility(new_ops, columns, bitmap);
                    yield Message::Chunk(new_chunk);
                }
                m => yield m,
            }
        }
    }
}
