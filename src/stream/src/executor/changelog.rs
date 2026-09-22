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
use indexmap::IndexMap;
use itertools::Itertools;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{ArrayImpl, I16Array, Op, SerialArray, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl, Serial};
use risingwave_common::util::row_id::ChangelogRowIdGenerator;

use super::{ActorContextRef, BoxedMessageStream, Execute, Executor, Message, StreamExecutorError};

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

    /// Orders changelog events so that the latest event per business key represents its state at the barrier.
    /// Intermediate replacements may be reordered.
    ///
    /// Consider the schema (id, tier), with business key (id) and stream key (id, tier).
    ///
    /// Upstream actors can be partitioned by (id, tier), so a replacement's insert may
    /// arrive before the old row's delete.
    ///
    /// Consider the following events:
    ///
    /// ```text
    /// 1. + (55, Plus)
    /// 2. + (88, Basic)
    /// 3. + (55, Pro)
    /// 4. + (88, Pro)
    /// 5. - (55, Plus)
    /// 6. - (88, Basic)
    /// ```
    ///
    /// Here, ID 55 transitions from Plus -> Pro,
    /// but the insertion for Pro comes before the deletion for Plus
    ///
    /// We solve this by buffering by business key, then by stream key, keeping each history's arrival order:
    ///
    /// ```text
    /// business key       stream key          history
    /// 55 ───────────┬── (55, Plus)  ────> [+, -]  ends in deletion
    ///               └── (55, Pro)   ────> [+]     survives
    ///
    /// 88 ───────────┬── (88, Basic) ────> [+, -]  ends in deletion
    ///               └── (88, Pro)   ────> [+]     survives
    /// ```
    ///
    /// At the barrier, emit deletion-ending histories before surviving histories.
    /// Move each history as a whole: its insert must still precede its own delete.
    ///
    /// ```text
    /// business key 55                    business key 88
    /// + (55, Plus) -> ID A1              + (88, Basic) -> ID B1
    /// - (55, Plus) -> ID A2              - (88, Basic) -> ID B2
    /// + (55, Pro)  -> ID A3              + (88, Pro)   -> ID B3
    ///                A1 < A2 < A3                       B1 < B2 < B3
    /// ```
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_keyed(mut self) {
        let mut builder = StreamChunkBuilder::new(
            self.ctx.config.developer.chunk_size,
            self.output_data_types(),
        );
        let mut buffer = EventBuffer::new();
        let input = self.input.execute();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        let [business_key, stream_key] =
                            [&self.distribution_keys, &self.stream_keys]
                                .map(|key| row.project(key).into_owned_row());

                        buffer
                            .entry(business_key)
                            .or_default()
                            .entry(stream_key)
                            .or_default()
                            .push((op, row.into_owned_row()));
                    }
                }
                Message::Watermark(_w) => {}
                Message::Barrier(barrier) => {
                    for (_key, group) in buffer.drain(..) {
                        let (vacating, remaining): (Vec<_>, Vec<_>) =
                            group.into_values().partition(|event_history| {
                                event_history.last().is_some_and(|(op, _row)| {
                                    matches!(op, Op::Delete | Op::UpdateDelete)
                                })
                            });

                        // rows with the same keys are grouped together, so they can share the same vnode value
                        // this way we can avoid recomputing it many times for the same value
                        let mut vnode = None;

                        for (op, row) in vacating.into_iter().chain(remaining).flatten() {
                            let vnode = vnode.get_or_insert_with(|| {
                                VirtualNode::compute_row(
                                    &row,
                                    &self.distribution_keys,
                                    self.all_vnode_count,
                                )
                            });
                            let id = Serial::from(self.changelog_row_id_generator.next(vnode));

                            let row = {
                                let mut datums = row.into_inner().into_vec();

                                if self.need_op {
                                    datums.push(Some(ScalarImpl::Int16(op.to_i16())));
                                }

                                datums.push(Some(ScalarImpl::Serial(id)));

                                OwnedRow::new(datums)
                            };

                            if let Some(chunk) = builder.append_row(Op::Insert, row) {
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

type EventHistory = Vec<(Op, OwnedRow)>;

/// Events buffered during one barrier interval, grouped by declared key and then by input stream key
type EventBuffer = IndexMap<OwnedRow, IndexMap<OwnedRow, EventHistory>>;

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

        let [original_insert_id, old_delete_id, replacement_insert_id] = ids[..] else {
            panic!("expected exactly three changelog events");
        };

        assert!(
            original_insert_id < old_delete_id,
            "original insert must precede deletion"
        );

        assert!(
            old_delete_id < replacement_insert_id,
            "old deletion must precede replacement insert"
        );
    }
}
