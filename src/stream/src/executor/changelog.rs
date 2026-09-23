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
use itertools::Itertools;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{ArrayImpl, I16Array, Op, SerialArray, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, RowExt};
use risingwave_common::types::{DataType, ScalarImpl, Serial};
use risingwave_common::util::row_id::ChangelogRowIdGenerator;

use super::{ActorContextRef, BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};
use crate::common::change_buffer::InconsistencyBehavior;
use crate::common::change_buffer::output_kind::RETRACT;
use crate::common::compact_chunk::StreamChunkCompactor;

pub struct ChangeLogExecutor {
    ctx: ActorContextRef,
    input: Executor,
    need_op: bool,
    all_vnode_count: usize,
    mode: ChangeLogMode,
    distribution_keys: Vec<usize>,
    stream_keys: Vec<usize>,
    changelog_row_id_generator: ChangelogRowIdGenerator,
}

impl Execute for ChangeLogExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        if matches!(self.mode, ChangeLogMode::Keyed) {
            self.execute_keyed().boxed()
        } else {
            self.execute_normal().boxed()
        }
    }
}

impl ChangeLogExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        need_op: bool,
        all_vnode_count: usize,
        vnodes: Bitmap,
        mode: ChangeLogMode,
        distribution_keys: Vec<usize>,
        stream_keys: Vec<usize>,
    ) -> Self {
        let changelog_row_id_generator = ChangelogRowIdGenerator::new(vnodes, all_vnode_count);
        Self {
            ctx,
            input,
            need_op,
            all_vnode_count,
            mode,
            distribution_keys,
            stream_keys,
            changelog_row_id_generator,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_normal(mut self) {
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

                    if self.need_op {
                        let ops = ops.iter().map(|op| Some(op.to_i16())).collect_vec();
                        let ops_array = Arc::new(ArrayImpl::Int16(I16Array::from_iter(ops)));

                        columns.push(ops_array);
                    }

                    let changelog_row_ids = vnodes
                        .iter()
                        .map(|vnode| self.changelog_row_id_generator.next(vnode));
                    let changelog_row_id_array = Arc::new(ArrayImpl::Serial(
                        SerialArray::from_iter(changelog_row_ids.map(Serial::from)),
                    ));

                    columns.push(changelog_row_id_array);

                    let new_ops = vec![Op::Insert; ops.len()];
                    let new_chunk = StreamChunk::with_visibility(new_ops, columns, bitmap);

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

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_keyed(mut self) {
        let mut builder = StreamChunkBuilder::new(
            self.ctx.config.developer.chunk_size,
            self.output_data_types(),
        );
        let mut chunk_buffer: Vec<StreamChunk> = vec![];
        let input = self.input.execute();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => chunk_buffer.push(chunk),
                Message::Watermark(_w) => {}
                Message::Barrier(barrier) => {
                    let chunks = StreamChunkCompactor::new(
                        self.stream_keys.clone(),
                        std::mem::take(&mut chunk_buffer),
                    )
                    .into_compacted_chunks_inline::<RETRACT>(InconsistencyBehavior::Warn);

                    let mut deletes = Vec::with_capacity(chunks.len());
                    let mut inserts = Vec::with_capacity(chunks.len());

                    for chunk in chunks {
                        let vnodes: Arc<[VirtualNode]> = VirtualNode::compute_chunk(
                            chunk.data_chunk(),
                            &self.distribution_keys,
                            self.all_vnode_count,
                        )
                        .into();

                        let delete_view = chunk.clone().retain_ops(&[Op::Delete, Op::UpdateDelete]);
                        if delete_view.any() {
                            deletes.push((delete_view, vnodes.clone()));
                        }

                        let insert_view = chunk.retain_ops(&[Op::Insert, Op::UpdateInsert]);
                        if insert_view.any() {
                            inserts.push((insert_view, vnodes));
                        }
                    }

                    for (chunk, vnodes) in deletes.into_iter().chain(inserts) {
                        for (event, vnode) in chunk.rows_with_holes().zip(vnodes.iter()) {
                            let Some((op, row)) = event else { continue };

                            let id = Serial::from(self.changelog_row_id_generator.next(vnode));

                            let mut suffix = Vec::with_capacity(2);

                            if self.need_op {
                                suffix.push(Some(ScalarImpl::Int16(op.to_i16())));
                            }

                            suffix.push(Some(ScalarImpl::Serial(id)));

                            if let Some(chunk) =
                                builder.append_row(Op::Insert, row.chain(OwnedRow::new(suffix)))
                            {
                                yield Message::Chunk(chunk);
                            }
                        }
                    }

                    if let Some(chunk) = builder.take() {
                        yield Message::Chunk(chunk);
                    }

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

    fn output_data_types(&self) -> Vec<DataType> {
        let mut data_types = self.input.schema().data_types();

        if self.need_op {
            data_types.push(DataType::Int16);
        }

        data_types.push(DataType::Serial);
        data_types
    }
}

#[derive(Debug)]
pub enum ChangeLogMode {
    Normal,
    Keyed,
}

impl Debug for ChangeLogExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeLogExecutor").finish()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::{Row, RowExt};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarRefImpl};
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, Barrier};

    #[tokio::test]
    async fn test_keyed_changelog_reorders_replacement() {
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty("I T B\n+ 42 Beginner false")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty("I T B\n+ 42 Advanced true")),
            Message::Chunk(StreamChunk::from_pretty("I T B\n- 42 Beginner false")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .stop_on_finish(false)
        .into_executor(
            Schema::new(vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Boolean),
            ]),
            vec![0, 1, 2],
        );

        let mut output = ChangeLogExecutor::new(
            ActorContext::for_test(1),
            source,
            true,
            VirtualNode::COUNT_FOR_TEST,
            Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
            ChangeLogMode::Keyed,
            vec![0],
            vec![0, 1, 2],
        )
        .boxed()
        .execute();

        let mut events = Vec::new();

        while let Some(message) = output.next().await {
            match message.unwrap() {
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        assert_eq!(op, Op::Insert, "changelog output must be append-only");

                        let Some(ScalarRefImpl::Serial(id)) = row.datum_at(4) else {
                            panic!("expected a non-null changelog event ID");
                        };

                        let Some(ScalarRefImpl::Int16(change_op)) = row.datum_at(3) else {
                            panic!("expected a non-null changelog operation");
                        };

                        let row = row.project(&[0, 1, 2]).into_owned_row();

                        events.push((row, change_op, id));
                    }
                }
                Message::Barrier(_) => {}
                Message::Watermark(_) => panic!("unexpected watermark"),
            }
        }

        let expected = StreamChunk::from_pretty(
            "I T B\n+ 42 Beginner false\n- 42 Beginner false\n+ 42 Advanced true",
        );
        let ids = expected
            .rows()
            .map(|(op, row)| {
                let row = row.to_owned_row();

                events
                    .iter()
                    .find(|(event, change_op, _)| *event == row && *change_op == op.to_i16())
                    .unwrap_or_else(|| panic!("missing changelog event: {row:?}"))
                    .2
            })
            .collect_vec();

        let [old_insert_id, old_delete_id, new_insert_id] = ids[..] else {
            panic!("expected exactly three changelog events");
        };

        assert!(
            old_insert_id < old_delete_id,
            "original insert must precede deletion"
        );

        assert!(
            old_delete_id < new_insert_id,
            "old deletion must precede replacement insert"
        );
    }
}
