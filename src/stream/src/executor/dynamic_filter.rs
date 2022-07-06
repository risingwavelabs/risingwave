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

use std::collections::BTreeMap;
use std::ops::Bound::*;
use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use madsim::collections::HashSet;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{Datum, ScalarImpl, ToOwnedDatum};
use risingwave_expr::expr::BoxedExpression;
use risingwave_pb::expr::expr_node::Type as ExprNodeType;
use risingwave_pb::expr::expr_node::Type::*;

use super::barrier_align::*;
use super::error::StreamExecutorError;
use super::monitor::StreamingMetrics;
use super::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef};
use crate::common::StreamChunkBuilder;
use crate::executor::PROCESSING_WINDOW_SIZE;

pub struct DynamicFilterExecutor {
    source_l: BoxedExecutor,
    source_r: BoxedExecutor,
    key_l: usize,
    pk_indices: PkIndices,
    identity: String,
    cond: BoxedExpression,
    comparator: ExprNodeType,
    actor_id: u64,
    schema: Schema,
    metrics: Arc<StreamingMetrics>,
}

impl DynamicFilterExecutor {
    pub fn new(
        source_l: BoxedExecutor,
        source_r: BoxedExecutor,
        key_l: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        cond: BoxedExpression,
        comparator: ExprNodeType,
        actor_id: u64,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let schema = source_l.schema().clone();
        Self {
            source_l,
            source_r,
            key_l,
            pk_indices,
            identity: format!("DynamicFilterExecutor {:X}", executor_id),
            cond,
            comparator,
            actor_id,
            metrics,
            schema,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(self) {
        let mut prev_epoch_value: Option<Datum> = None;
        let mut current_epoch_value: Option<Datum> = None;

        // The state is sorted by the comparison value
        //
        // TODO: convert this into a `StateTable` compatible managed state
        //
        // TODO: It could be potentially expensive memory-wise to store `HashSet`.
        //       If I'm not wrong, the memory overhead is a backing Vec of size 4
        //       (See: https://github.com/rust-lang/hashbrown/pull/162)
        //       + some byte-per-entry metadata. Well, `Row` is on heap anyway...
        //       It could be preferred to find a way to do prefix range scans on the left key and
        //       storing as `BTreeSet<(ScalarImpl, Row)>`.
        //       We could solve it if `ScalarImpl` had a successor/predecessor function.
        //
        //       Probably, using a custom comparator function on a custom datatype
        //       that is equivalent in all `Row` can achieve this. We are completely agnostic
        //       about the ordering of `Row`
        let mut state = BTreeMap::<ScalarImpl, HashSet<Row>>::new();

        let input_l = self.source_l;
        let input_r = self.source_r;
        let aligned_stream = barrier_align(
            input_l.execute(),
            input_r.execute(),
            self.actor_id,
            self.metrics.clone(),
        );

        let mut stream_chunk_builder =
            StreamChunkBuilder::new(PROCESSING_WINDOW_SIZE, &self.schema.data_types(), 0, 0)
                .map_err(StreamExecutorError::eval_error)?;

        #[for_await]
        for msg in aligned_stream {
            match msg? {
                AlignedMessage::Left(chunk) => {
                    // TODO: refactor into fn: `apply_batch`
                    // Reuse the logic from `FilterExecutor`
                    let chunk = chunk.compact()?; // Is this unnecessary work?
                    let (data_chunk, ops) = chunk.into_parts();

                    let mut new_ops = Vec::with_capacity(ops.len());
                    let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
                    let mut last_res = false;

                    for (row, op) in data_chunk.rows().zip_eq(ops.iter()) {
                        // TODO: convert this to a batch operation?
                        let left_val = row.value_at(self.key_l).to_owned_datum();

                        // Evaluate the condition and determine if it should be forwarded
                        if let Some(right_val) = &prev_epoch_value {
                            let inputs = Row::new(vec![left_val.clone(), right_val.clone()]);

                            // If the condition evaluates to true, we forward it
                            // TODO: optimization - allow eval on left side data chunk,
                            // right side Datum
                            let res = self
                                .cond
                                .eval_row(&inputs)?
                                .map(|r| *r.as_bool())
                                .unwrap_or(false);

                            match *op {
                                Op::Insert | Op::Delete => {
                                    new_ops.push(*op);
                                    if res {
                                        new_visibility.append(true);
                                    } else {
                                        new_visibility.append(false);
                                    }
                                }
                                Op::UpdateDelete => {
                                    last_res = res;
                                }
                                Op::UpdateInsert => match (last_res, res) {
                                    (true, false) => {
                                        new_ops.push(Op::Delete);
                                        new_ops.push(Op::UpdateInsert);
                                        new_visibility.append(true);
                                        new_visibility.append(false);
                                    }
                                    (false, true) => {
                                        new_ops.push(Op::UpdateDelete);
                                        new_ops.push(Op::Insert);
                                        new_visibility.append(false);
                                        new_visibility.append(true);
                                    }
                                    (true, true) => {
                                        new_ops.push(Op::UpdateDelete);
                                        new_ops.push(Op::UpdateInsert);
                                        new_visibility.append(true);
                                        new_visibility.append(true);
                                    }
                                    (false, false) => {
                                        new_ops.push(Op::UpdateDelete);
                                        new_ops.push(Op::UpdateInsert);
                                        new_visibility.append(false);
                                        new_visibility.append(false);
                                    }
                                },
                            }
                        }

                        // Store the rows without a null left key
                        // null key in left side of predicate should never be stored
                        // (it will never satisfy the filter condition)
                        if let Some(val) = left_val {
                            match *op {
                                Op::Insert | Op::UpdateInsert => {
                                    let entry = state.entry(val).or_insert_with(HashSet::new);
                                    entry.insert(row.to_owned_row());
                                }
                                Op::Delete | Op::UpdateDelete => {
                                    let contains_element = state
                                        .get_mut(&val)
                                        .ok_or(StreamExecutorError::from(anyhow!(
                                            "Deleting non-existent element"
                                        )))?
                                        .remove(&row.to_owned_row());

                                    if !contains_element {
                                        return Err(StreamExecutorError::from(anyhow!(
                                            "Deleting non-existent element"
                                        )));
                                    }
                                }
                            }
                        }
                    }
                    let new_visibility = new_visibility.finish();

                    let (columns, _) = data_chunk.into_parts();

                    if new_visibility.num_high_bits() > 0 {
                        let new_chunk = StreamChunk::new(new_ops, columns, Some(new_visibility));
                        yield Message::Chunk(new_chunk)
                    }
                }
                AlignedMessage::Right(chunk) => {
                    // Store the latest update to the right value
                    // (This should eventually be persisted via `StateTable` as well - at the
                    // barrier)
                    let chunk = chunk.compact()?; // Is this unnecessary work?
                    let (data_chunk, ops) = chunk.into_parts();

                    let mut last_is_insert = true;
                    for (row, op) in data_chunk.rows().zip_eq(ops.iter()) {
                        match *op {
                            Op::UpdateInsert | Op::Insert => {
                                last_is_insert = true;
                                current_epoch_value = Some(row.value_at(0).to_owned_datum());
                            }
                            _ => last_is_insert = false,
                        }
                    }

                    // Alternatively, the behaviour can be to flatten the deletion of
                    // `current_epoch_value` into a NULL represented by a `None: Datum`
                    if !last_is_insert {
                        return Err(anyhow!("RHS updates should always end with inserts").into());
                    }
                }
                AlignedMessage::Barrier(barrier) => {
                    // Flush the difference between the `prev_value` and `current_value`
                    // TODO: refactor into fn `flush_range`
                    let curr: Datum = current_epoch_value.clone().flatten();
                    let prev: Datum = prev_epoch_value.flatten();
                    if prev != curr {
                        let curr_is_some = curr.is_some();
                        let (range, is_insert) = match (curr.clone(), prev) {
                            (Some(c), None) | (None, Some(c)) => {
                                let range = match self.comparator {
                                    GreaterThan => (Excluded(c), Unbounded),
                                    GreaterThanOrEqual => (Included(c), Unbounded),
                                    LessThan => (Unbounded, Excluded(c)),
                                    LessThanOrEqual => (Unbounded, Included(c)),
                                    _ => unreachable!(),
                                };
                                let is_insert = curr_is_some;
                                (range, is_insert)
                            }
                            (Some(c), Some(p)) => {
                                if c < p {
                                    let range = match self.comparator {
                                        GreaterThan | LessThan => (Excluded(c), Excluded(p)),
                                        GreaterThanOrEqual | LessThanOrEqual => {
                                            (Included(c), Included(p))
                                        }
                                        _ => unreachable!(),
                                    };
                                    let is_insert =
                                        matches!(self.comparator, GreaterThan | GreaterThanOrEqual);
                                    (range, is_insert)
                                } else {
                                    // p > c
                                    let range = match self.comparator {
                                        GreaterThan | LessThan => (Excluded(p), Excluded(c)),
                                        GreaterThanOrEqual | LessThanOrEqual => {
                                            (Included(p), Included(c))
                                        }
                                        _ => unreachable!(),
                                    };
                                    let is_insert =
                                        matches!(self.comparator, LessThan | LessThanOrEqual);
                                    (range, is_insert)
                                }
                            }
                            (None, None) => unreachable!(), // prev != curr
                        };

                        for (_, rows) in state.range(range) {
                            for row in rows {
                                if let Some(chunk) = stream_chunk_builder
                                    .append_row_matched(
                                        // All rows have a single identity at this point
                                        if is_insert { Op::Insert } else { Op::Delete },
                                        row,
                                    )
                                    .map_err(StreamExecutorError::eval_error)?
                                {
                                    yield Message::Chunk(chunk);
                                }
                            }
                        }
                        if let Some(chunk) = stream_chunk_builder
                            .take()
                            .map_err(StreamExecutorError::eval_error)?
                        {
                            yield Message::Chunk(chunk);
                        }
                    }
                    // TODO: We will persist `prev_epoch_value` to the `StateTable` as well
                    prev_epoch_value = Some(curr);
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl Executor for DynamicFilterExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

// TODO: unit tests - test each comparator. With inserts and deletes. With updates on RHS.
