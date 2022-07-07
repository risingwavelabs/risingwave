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
use std::ops::Bound;
use std::ops::Bound::*;
use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use madsim::collections::HashSet;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Array, ArrayImpl, DataChunk, Op, Row, StreamChunk, Vis};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, Datum, ScalarImpl, ToOwnedDatum};
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
    source_l: Option<BoxedExecutor>,
    source_r: Option<BoxedExecutor>,
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
    #[allow(clippy::too_many_arguments)]
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
            source_l: Some(source_l),
            source_r: Some(source_r),
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

    fn apply_batch(
        &self,
        data_chunk: &DataChunk,
        ops: Vec<Op>,
        prev_epoch_value: &Option<ScalarImpl>,
        right_data_type: &DataType,
        state: &mut BTreeMap<ScalarImpl, HashSet<Row>>,
    ) -> Result<(Vec<Op>, Bitmap), StreamExecutorError> {
        debug_assert_eq!(ops.len(), data_chunk.cardinality());
        let mut new_ops = Vec::with_capacity(ops.len());
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        let mut last_res = false;

        let left_column = data_chunk.column_at(self.key_l);

        let eval_results = if let Some(right_val) = &prev_epoch_value {
            let mut eval_columns = Vec::with_capacity(2);
            eval_columns.push(left_column.clone());

            // TODO: could optimize this - creating a repeated array could be much cheaper.
            let mut array_builder = right_data_type.create_array_builder(ops.len());
            for _ in 0..ops.len() {
                array_builder.append_datum(&Some(right_val.clone()))?;
            }
            let right_column = Column::new(Arc::new(array_builder.finish()?));
            eval_columns.push(right_column);
            let eval_data_chunk = DataChunk::new(eval_columns, Vis::Compact(ops.len()));
            Some(self.cond.eval(&eval_data_chunk)?)
        } else {
            None
        };

        for (idx, (row, op)) in data_chunk.rows().zip_eq(ops.iter()).enumerate() {
            let left_val = row.value_at(self.key_l).to_owned_datum();

            let res = if let Some(array) = &eval_results {
                if let ArrayImpl::Bool(results) = &**array {
                    results.value_at(idx).unwrap_or(false)
                } else {
                    panic!("condition eval must return bool array")
                }
            } else {
                // A NULL right value implies a false evaluation for all rows
                false
            };

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
                            .ok_or_else(|| {
                                StreamExecutorError::from(anyhow!("Deleting non-existent element"))
                            })?
                            .remove(&row.to_owned_row());

                        if !contains_element {
                            return Err(StreamExecutorError::from(anyhow!(
                                "Deleting non-existent element"
                            )));
                        };
                    }
                }
            }
        }

        let new_visibility = new_visibility.finish();

        Ok((new_ops, new_visibility))
    }

    fn get_range(
        &self,
        curr: &Datum,
        prev: Datum,
    ) -> ((Bound<ScalarImpl>, Bound<ScalarImpl>), bool) {
        debug_assert_ne!(curr, &prev);
        let curr_is_some = curr.is_some();
        match (curr.clone(), prev) {
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
                        GreaterThanOrEqual | LessThanOrEqual => (Included(c), Included(p)),
                        _ => unreachable!(),
                    };
                    let is_insert = matches!(self.comparator, GreaterThan | GreaterThanOrEqual);
                    (range, is_insert)
                } else {
                    // p > c
                    let range = match self.comparator {
                        GreaterThan | LessThan => (Excluded(p), Excluded(c)),
                        GreaterThanOrEqual | LessThanOrEqual => (Included(p), Included(c)),
                        _ => unreachable!(),
                    };
                    let is_insert = matches!(self.comparator, LessThan | LessThanOrEqual);
                    (range, is_insert)
                }
            }
            (None, None) => unreachable!(), // prev != curr
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut prev_epoch_value: Option<Datum> = None;
        let mut current_epoch_value: Option<Datum> = None;

        // The state is sorted by the comparison value
        //
        // TODO: convert this into a `StateTable` compatible managed state
        //
        // TODO: It could be potentially expensive memory-wise to store `HashSet`.
        //       The memory overhead per single row is potentially a backing Vec of size 4
        //       (See: https://github.com/rust-lang/hashbrown/pull/162)
        //       + some byte-per-entry metadata. Well, `Row` is on heap anyway...
        //
        //       It could be preferred to find a way to do prefix range scans on the left key and
        //       storing as `BTreeSet<(ScalarImpl, Row)>`.
        //       We could solve it if `ScalarImpl` had a successor/predecessor function.
        let mut state = BTreeMap::<ScalarImpl, HashSet<Row>>::new();

        let input_l = self.source_l.take().unwrap();
        let input_r = self.source_r.take().unwrap();
        let right_data_type = input_r.schema().data_types()[0].clone();
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
                    // Reuse the logic from `FilterExecutor`
                    let chunk = chunk.compact()?; // Is this unnecessary work?
                    let (data_chunk, ops) = chunk.into_parts();

                    let (new_ops, new_visibility) = self.apply_batch(
                        &data_chunk,
                        ops,
                        &prev_epoch_value.clone().flatten(),
                        &right_data_type,
                        &mut state,
                    )?;

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
                    let curr: Datum = current_epoch_value.clone().flatten();
                    let prev: Datum = prev_epoch_value.flatten();
                    if prev != curr {
                        let (range, is_insert) = self.get_range(&curr, prev);
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
