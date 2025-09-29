// Copyright 2025 RisingWave Labs
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
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::Serial;
use risingwave_common::util::row_id::ChangelogRowIdGenerator;

use super::{ActorContextRef, BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};

pub struct ChangeLogExecutor {
    ctx: ActorContextRef,
    input: Executor,
    need_op: bool,
    all_vnode_count: usize,
    distribution_keys: Vec<usize>,
    changelog_row_id_generator: ChangelogRowIdGenerator,
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
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        need_op: bool,
        all_vnode_count: usize,
        vnodes: Bitmap,
        distribution_keys: Vec<usize>,
    ) -> Self {
        let changelog_row_id_generator = ChangelogRowIdGenerator::new(vnodes, all_vnode_count);
        Self {
            ctx,
            input,
            need_op,
            all_vnode_count,
            distribution_keys,
            changelog_row_id_generator,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let input = self.input.execute();
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let data_chunk = chunk.data_chunk();
                    let vnodes = VirtualNode::compute_chunk(
                        data_chunk,
                        &self.distribution_keys,
                        self.all_vnode_count,
                    );
                    let (ops, mut columns, bitmap) = chunk.into_inner();
                    let new_ops = vec![Op::Insert; ops.len()];
                    let changelog_row_ids = vnodes
                        .iter()
                        .map(|vnode| self.changelog_row_id_generator.next(vnode));
                    let changelog_row_id_array = Arc::new(ArrayImpl::Serial(
                        SerialArray::from_iter(changelog_row_ids.map(Serial::from)),
                    ));
                    let new_chunk = if self.need_op {
                        let ops: Vec<Option<i16>> =
                            ops.iter().map(|op| Some(op.to_i16())).collect();
                        let ops_array = Arc::new(ArrayImpl::Int16(I16Array::from_iter(ops)));
                        columns.push(ops_array);
                        columns.push(changelog_row_id_array);
                        StreamChunk::with_visibility(new_ops, columns, bitmap)
                    } else {
                        columns.push(changelog_row_id_array);
                        StreamChunk::with_visibility(new_ops, columns, bitmap)
                    };
                    yield Message::Chunk(new_chunk);
                }
                Message::Watermark(_w) => {}
                Message::Barrier(barrier) => {
                    if let Some(vnodes) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        self.changelog_row_id_generator = ChangelogRowIdGenerator::new(
                            vnodes.as_ref().clone(),
                            self.all_vnode_count,
                        );
                    }
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}
