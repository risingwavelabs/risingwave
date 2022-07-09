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

use std::ops::Bound::{self, *};
use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use madsim::collections::btree_map::Range;
use madsim::collections::{BTreeMap, HashSet};
use risingwave_common::array::{Array, ArrayImpl, DataChunk, Op, Row, StreamChunk};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, Datum, ScalarImpl, ToOwnedDatum};
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{BoxedExpression, InputRefExpression, LiteralExpression};
use risingwave_pb::expr::expr_node::Type as ExprNodeType;
use risingwave_pb::expr::expr_node::Type::*;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::barrier_align::*;
use super::error::{StreamExecutorError, StreamExecutorResult};
use super::monitor::StreamingMetrics;
use super::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef};
use crate::common::StreamChunkBuilder;
use crate::executor::PROCESSING_WINDOW_SIZE;

pub struct RangeCache<S: StateStore> {
    // TODO: It could be potentially expensive memory-wise to store `HashSet`.
    //       The memory overhead per single row is potentially a backing Vec of size 4
    //       (See: https://github.com/rust-lang/hashbrown/pull/162)
    //       + some byte-per-entry metadata. Well, `Row` is on heap anyway...
    //
    //       It could be preferred to find a way to do prefix range scans on the left key and
    //       storing as `BTreeSet<(ScalarImpl, Row)>`.
    //       We could solve it if `ScalarImpl` had a successor/predecessor function.
    cache: BTreeMap<ScalarImpl, HashSet<Row>>,
    state_table: StateTable<S>,
    /// The current range stored in the cache.
    /// Any request for a set of values outside of this range will result in a scan
    /// from storage
    range: (Bound<ScalarImpl>, Bound<ScalarImpl>),

    #[allow(unused)]
    num_rows_stored: usize,
    #[allow(unused)]
    capacity: usize,
    current_epoch: u64,
}

type ScalarRange = (Bound<ScalarImpl>, Bound<ScalarImpl>);

impl<S: StateStore> RangeCache<S> {
    pub fn new(state_table: StateTable<S>, current_epoch: u64) -> Self {
        Self {
            cache: BTreeMap::new(),
            state_table,
            range: (Unbounded, Unbounded),
            num_rows_stored: 0,
            capacity: usize::MAX,
            current_epoch,
        }
    }

    pub fn insert(&mut self, k: ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if self.range.contains(&k) {
            let entry = self.cache.entry(k).or_insert_with(HashSet::new);
            entry.insert(v.clone());
        }
        self.state_table.insert(v)?;
        Ok(())
    }

    pub fn delete(&mut self, k: &ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if self.range.contains(k) {
            let contains_element = self
                .cache
                .get_mut(k)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?
                .remove(&v);

            if !contains_element {
                return Err(StreamExecutorError::from(anyhow!(
                    "Deleting non-existent element"
                )));
            };
        }
        self.state_table.delete(v)?;
        Ok(())
    }

    pub fn range(&self, range: ScalarRange) -> Range<ScalarImpl, HashSet<Row>> {
        // What we want: At the end of every epoch we will try to read
        // ranges based on the new value. The values in the range may not all be cached.
        //
        // If the new range is overlapping with the current range, we will keep the
        // current range. We will then evict to capacity after the cache has been populated and
        // the
        //
        // Actually, we don't really need to return `Range` as all we really need is an iterator
        // over rows.
        //
        // Here is our strategy for populating the cache:
        //
        // We will always cache towards the direction of the previous value's movement.
        //
        // Time | Scenario
        // -----+---------------------------------------------
        //   1  | [--cached range--]        *<--prev_value
        //      |                       [--requested range--]
        //      |
        //
        // If this requested range is too large, it will cause OOM.
        //
        // --------------------------------------------------------------------
        //
        // For overlapping ranges, we will prevent double inserts,
        // preferring the fresher in-cache value
        //
        // let lower_fetch_range: Option<ScalarRange>) = match self.range.0 {
        //     Unbounded => None,
        //     Included(x) | Excluded(x) => match range.0 {
        //         Unbounded => (Unbounded, Included(x)),
        //         bound @ Included(y) | Excluded(y) => if y
        //         Included(y) | Excluded(y) => x <= y,
        //     },
        //     Excluded(x) =>
        // }

        self.cache.range(range)
    }

    pub async fn flush(&mut self) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state_table.commit(self.current_epoch).await?;
        Ok(())
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }
}

pub struct DynamicFilterExecutor<S: StateStore> {
    source_l: Option<BoxedExecutor>,
    source_r: Option<BoxedExecutor>,
    key_l: usize,
    pk_indices: PkIndices,
    identity: String,
    comparator: ExprNodeType,
    range_cache: RangeCache<S>,
    actor_id: u64,
    schema: Schema,
    metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> DynamicFilterExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_l: BoxedExecutor,
        source_r: BoxedExecutor,
        key_l: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        comparator: ExprNodeType,
        state_table_l: StateTable<S>,
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
            comparator,
            range_cache: RangeCache::new(state_table_l, 0),
            actor_id,
            metrics,
            schema,
        }
    }

    fn apply_batch(
        &mut self,
        data_chunk: &DataChunk,
        ops: Vec<Op>,
        condition: Option<BoxedExpression>,
    ) -> Result<(Vec<Op>, Bitmap), StreamExecutorError> {
        debug_assert_eq!(ops.len(), data_chunk.cardinality());
        let mut new_ops = Vec::with_capacity(ops.len());
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        let mut last_res = false;

        let eval_results = if let Some(cond) = condition {
            Some(cond.eval(data_chunk)?)
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
                        self.range_cache.insert(val, row.to_owned_row())?;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.range_cache.delete(&val, row.to_owned_row())?;
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
        let input_l = self.source_l.take().unwrap();
        let input_r = self.source_r.take().unwrap();
        // Derive the dynamic expression
        let l_data_type = input_l.schema().data_types()[self.key_l].clone();
        let r_data_type = input_r.schema().data_types()[0].clone();
        let dynamic_cond = move |literal: Datum| -> Option<BoxedExpression> {
            literal.map(|scalar| {
                new_binary_expr(
                    self.comparator,
                    DataType::Boolean,
                    Box::new(InputRefExpression::new(l_data_type.clone(), self.key_l)),
                    Box::new(LiteralExpression::new(r_data_type.clone(), Some(scalar))),
                )
            })
        };

        let mut prev_epoch_value: Option<Datum> = None;
        let mut current_epoch_value: Option<Datum> = None;

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

                    let right_val = prev_epoch_value.clone().flatten();

                    // The condition is `None` if it is always false by virtue of a NULL right
                    // input, so we save evaluating it on the datachunk
                    let condition = dynamic_cond(right_val);

                    let (new_ops, new_visibility) =
                        self.apply_batch(&data_chunk, ops, condition)?;

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
                        for (_, rows) in self.range_cache.range(range) {
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

                    self.range_cache.flush().await?;
                    self.range_cache.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl<S: StateStore> Executor for DynamicFilterExecutor<S> {
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
