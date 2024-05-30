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
use risingwave_common::array::{ArrayImpl, I16Array, Op, SerialArray, StreamChunk};
use risingwave_common::types::Serial;

use super::{ActorContextRef, BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};

pub struct ChangeLogExecutor {
    _ctx: ActorContextRef,
    input: Executor,
    need_op: bool,
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
    pub fn new(ctx: ActorContextRef, input: Executor, need_op: bool) -> Self {
        Self {
            _ctx: ctx,
            input,
            need_op,
        }
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
                    // They are all 0, will be add in row id gen executor.
                    let changed_log_row_id_array =
                        Arc::new(ArrayImpl::Serial(SerialArray::from_iter(vec![
                            Serial::from(
                                0
                            );
                            ops.len()
                        ])));
                    let new_chunk = if self.need_op {
                        let ops: Vec<Option<i16>> =
                            ops.iter().map(|op| Some(op.to_i16())).collect();
                        let ops_array = Arc::new(ArrayImpl::Int16(I16Array::from_iter(ops)));
                        let mut new_columns = Vec::with_capacity(columns.len() + 2);
                        new_columns.extend_from_slice(&columns);
                        new_columns.push(ops_array);
                        new_columns.push(changed_log_row_id_array);
                        StreamChunk::with_visibility(new_ops, new_columns, bitmap)
                    } else {
                        let mut new_columns = Vec::with_capacity(columns.len() + 1);
                        new_columns.extend_from_slice(&columns);
                        new_columns.push(changed_log_row_id_array);
                        StreamChunk::with_visibility(new_ops, new_columns, bitmap)
                    };
                    yield Message::Chunk(new_chunk);
                }
                m => yield m,
            }
        }
    }
}
