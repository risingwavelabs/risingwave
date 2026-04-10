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
use std::ops::Bound;
use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::NonStrictExpression;
use risingwave_storage::StateStore;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::store::PrefetchOptions;

use super::join::{JoinType, JoinTypePrimitive};
use super::temporal_join::{InternalMessage, align_input, apply_indices_map, phase1};
use super::{Execute, ExecutorInfo, Message, StreamExecutorError};
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::ReplicatedStateTable;
use crate::executor::join::builder::JoinStreamChunkBuilder;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, Executor};

pub struct NestedLoopTemporalJoinExecutor<
    S: StateStore,
    SD: ValueRowSerde,
    const T: JoinTypePrimitive,
> {
    ctx: ActorContextRef,
    #[allow(dead_code)]
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: TemporalSide<S, SD>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    chunk_size: usize,
    // TODO: update metrics
    #[allow(dead_code)]
    metrics: Arc<StreamingMetrics>,
}

struct TemporalSide<S: StateStore, SD: ValueRowSerde> {
    source: ReplicatedStateTable<S, SD>,
}

impl<S: StateStore, SD: ValueRowSerde> TemporalSide<S, SD> {}

#[try_stream(ok = StreamChunk, error = StreamExecutorError)]
#[allow(clippy::too_many_arguments)]
async fn phase1_handle_chunk<S: StateStore, SD: ValueRowSerde, E: phase1::Phase1Evaluation>(
    chunk_size: usize,
    right_size: usize,
    full_schema: Vec<DataType>,
    right_table: &mut TemporalSide<S, SD>,
    chunk: StreamChunk,
) {
    let mut builder = StreamChunkBuilder::new(chunk_size, full_schema);

    for (op, left_row) in chunk.rows() {
        let mut matched = false;
        for vnode in right_table.source.vnodes().iter_vnodes() {
            let iter = right_table
                .source
                .iter_with_vnode_and_output_indices(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    PrefetchOptions::prefetch_for_large_range_scan(),
                )
                .await?;
            #[for_await]
            for right_row in iter {
                let right_row = right_row?;
                matched = true;
                if let Some(chunk) = E::append_matched_row(op, &mut builder, left_row, right_row) {
                    yield chunk;
                }
            }
        }
        if let Some(chunk) = E::match_end(&mut builder, op, left_row, right_size, matched) {
            yield chunk;
        }
    }
    if let Some(chunk) = builder.take() {
        yield chunk;
    }
}

impl<S: StateStore, SD: ValueRowSerde, const T: JoinTypePrimitive>
    NestedLoopTemporalJoinExecutor<S, SD, T>
{
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        left: Executor,
        right: Executor,
        table: ReplicatedStateTable<S, SD>,
        condition: Option<NonStrictExpression>,
        output_indices: Vec<usize>,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> Self {
        let _metrics_info = MetricsInfo::new(
            metrics.clone(),
            table.table_id(),
            ctx.id,
            "nested loop temporal join",
        );

        Self {
            ctx,
            info,
            left,
            right,
            right_table: TemporalSide { source: table },
            condition,
            output_indices,
            chunk_size,
            metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let right_size = self.right.schema().len();

        let (left_map, _right_map) = JoinStreamChunkBuilder::get_i2o_mapping(
            &self.output_indices,
            self.left.schema().len(),
            right_size,
        );

        let left_to_output: HashMap<usize, usize> = HashMap::from_iter(left_map.iter().cloned());

        let full_schema: Vec<_> = self
            .left
            .schema()
            .data_types()
            .into_iter()
            .chain(self.right.schema().data_types().into_iter())
            .collect();

        let mut wait_first_barrier = true;

        #[for_await]
        for msg in align_input::<true>(self.left, self.right) {
            match msg? {
                InternalMessage::WaterMark(watermark) => {
                    let output_watermark_col_idx = *left_to_output.get(&watermark.col_idx).unwrap();
                    yield Message::Watermark(watermark.with_idx(output_watermark_col_idx));
                }
                InternalMessage::Chunk(chunk) => {
                    let full_schema = full_schema.clone();

                    if T == JoinType::Inner {
                        let st1 = phase1_handle_chunk::<S, SD, phase1::Inner>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            &mut self.right_table,
                            chunk,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = if let Some(ref cond) = self.condition {
                                let (data_chunk, ops) = chunk.into_parts();
                                let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                                let passed_bitmap =
                                    Arc::unwrap_or_clone(passed_bitmap).into_bool().to_bitmap();
                                let (columns, vis) = data_chunk.into_parts();
                                let new_vis = vis & passed_bitmap;
                                StreamChunk::with_visibility(ops, columns, new_vis)
                            } else {
                                chunk
                            };
                            let new_chunk = apply_indices_map(new_chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    } else if let Some(ref cond) = self.condition {
                        // Joined result without evaluating non-lookup conditions.
                        let st1 = phase1_handle_chunk::<S, SD, phase1::LeftOuterWithCond>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            &mut self.right_table,
                            chunk,
                        );
                        let mut matched_count = 0usize;
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let (data_chunk, ops) = chunk.into_parts();
                            let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                            let passed_bitmap =
                                Arc::unwrap_or_clone(passed_bitmap).into_bool().to_bitmap();
                            let (columns, vis) = data_chunk.into_parts();
                            let mut new_vis = BitmapBuilder::with_capacity(vis.len());
                            for (passed, not_match_end) in
                                passed_bitmap.iter().zip_eq_debug(vis.iter())
                            {
                                let is_match_end = !not_match_end;
                                let vis = if is_match_end && matched_count == 0 {
                                    // Nothing is matched, so the marker row should be visible.
                                    true
                                } else if is_match_end {
                                    // reset the count
                                    matched_count = 0;
                                    // rows found, so the marker row should be invisible.
                                    false
                                } else {
                                    if passed {
                                        matched_count += 1;
                                    }
                                    passed
                                };
                                new_vis.append(vis);
                            }
                            let new_chunk = apply_indices_map(
                                StreamChunk::with_visibility(ops, columns, new_vis.finish()),
                                &self.output_indices,
                            );
                            yield Message::Chunk(new_chunk);
                        }
                        // The last row should always be marker row,
                        assert_eq!(matched_count, 0);
                    } else {
                        let st1 = phase1_handle_chunk::<S, SD, phase1::LeftOuter>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            &mut self.right_table,
                            chunk,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = apply_indices_map(chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    }
                }
                InternalMessage::Barrier(updates, barrier) => {
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let barrier_epoch = barrier.epoch;

                    // Write right-side chunks to the replicated state table
                    for chunk in &updates {
                        self.right_table.source.write_chunk(chunk.clone());
                    }

                    if wait_first_barrier {
                        wait_first_barrier = false;
                        yield Message::Barrier(barrier);
                        self.right_table.source.init_epoch(barrier_epoch).await?;
                    } else {
                        let right_post_commit =
                            self.right_table.source.commit(barrier.epoch).await?;

                        yield Message::Barrier(barrier);

                        right_post_commit
                            .post_yield_barrier(update_vnode_bitmap)
                            .await?;
                    }
                }
            }
        }
    }
}

impl<S: StateStore, SD: ValueRowSerde, const T: JoinTypePrimitive> Execute
    for NestedLoopTemporalJoinExecutor<S, SD, T>
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }
}
