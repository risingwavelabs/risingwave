// Copyright 2023 RisingWave Labs
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

use std::cmp::Ordering;
use std::ops::Bound::{self, *};
use std::sync::Arc;

use futures::{pin_mut, stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Array, ArrayImpl, DataChunk, Op, RowRef, StreamChunk};
use risingwave_common::bail;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{once, OwnedRow as RowData, OwnedRow, Row, RowExt};
use risingwave_common::types::{
    DataType, Datum, DatumRef, DefaultOrd, ScalarImpl, ToDatumRef, ToOwnedDatum,
};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::{cmp_datum, cmp_datum_iter};
use risingwave_expr::expr::{build_func, BoxedExpression, InputRefExpression, LiteralExpression};
use risingwave_pb::expr::expr_node::Type as ExprNodeType;
use risingwave_pb::expr::expr_node::Type::{
    GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::barrier_align::*;
use super::error::StreamExecutorError;
use super::monitor::StreamingMetrics;
use super::{
    ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef,
};
use crate::common::table::state_table::StateTable;
use crate::common::StreamChunkBuilder;
use crate::executor::expect_first_barrier_from_aligned_stream;

/// # Monotonically Increasing Cache policy
/// Requirements:
/// 1. RHS must be monotonically increasing.
/// 2. Comparator must be >. i.e. LHS > RHS. e.g. v1 > NOW().
///
/// TODO: Remove both restrictions in the future.
///
/// ## Conventions
/// LHS: Outer side of the join.
/// RHS: Dynamic Filter (single value), inner side of the join.
///
/// ## Description
/// This cache is meant to minimize table scans of LHS internal table.
/// The cache holds a single value from the LHS,
/// the largest value nearest to current RHS (prioritize values larger than RHS).
/// By storing this value, we know for LHS values:
/// A) If LHS cache value is larger,
///    None are between LHS cache value and current RHS.
/// B) Or if there LHS cache value is same or smaller than current RHS.
///    None are larger than LHS cache value and current RHS value.
///
/// If RHS 100,
/// and LHS has the following values: 101, 102,
/// Cached value will be 101.
/// If LHS has the following values: 98 99,
/// Cached value will be 99.
///
/// On update, we need to do table scan, to decide which rows need to be sent downstream.
/// This happens because RHS value is dynamic.
/// So we need to send rows between (`old_RHS`, `current_RHS`) downstream for either
/// deletion/insertion. For monotonically increasing RHS (e.g. NOW()), `current_RHS` > `old_RHS`.
/// The `cache_value` from LHS partitions the range.
///
/// Case 1:
/// `cache_value` larger than `current_RHS`.
/// Need to send all rows between (`prev_RHS`, `current_RHS`) downstream.
///
/// Case 2:
/// `cache_value` smaller or same as `current_RHS`, larger than `prev_RHS`.
/// Need to send all rows between (`prev_RHS`, `cache_value`) downstream.
///
/// Case 3:
/// `cache_value` smaller than `prev_RHS`.
/// No need scan any rows, there are no LHS rows between (`prev_RHS`, `current_RHS`).
///
/// Case 4:
/// No `cache_value`.
/// Need to send all rows between (`prev_RHS`, `current_RHS`) downstream,
/// because cache value could have been deleted.
///
/// ## Initial state
/// Nothing in cache (None).
///
/// ## Updates
/// 1. Whenever we receive LHS updates, we need to update the cache.
///    `old_value` refers to `old_value` in cache.
///    `new_value` refers to the new value received from LHS in the update
///    message.
///
///    INSERT: if the new value is (RHS, `old_value`), replace `old_value` in cache.
///            Otherwise, do nothing.
///    UPDATE: if the new value is (RHS, `old_value`), OR if has same key as `old_value`,
///            replace `old_value` in cache.
///            Otherwise, do nothing.
///    DELETE: if the new value has the same key as `old_value`, empty cache. It is now `None`.
///
///    Example A:
///    `FILTER_COL`: [1]
///    Existing cache: Row(1, 3)
///    `RHS_VALUE`: Row(1, 1)
///    `LHS_INSERT`: Row(1, 2)
///    Cache update: Row(1, 3) -> Row(1, 2).
///
///    Example B:
///    `FILTER_COL`: [1]
///    Existing cache: Row(1, 3)
///    `RHS_VALUE`: Row(1, 1)
///    `LHS_DELETE`: Row(1, 3)
///    Cache update: Row(1, 3) -> None.
///
///    Example C:
///    `FILTER_COL`: [1]
///    Existing cache: Row(1, 3)
///    `RHS_VALUE`: Row(1, 1)
///    `LHS_INSERT`: Row(1, 1)
///    Cache update: No update, assuming condition is LHS > RHS.
///
/// 2. Whenever we receive RHS updates to our existing RHS value, we need to empty the cache
///    if the RHS value is not between existing RHS value and cache value.
///    Cache now holds `None`.
///
///    Example A:
///    `EXISTING_RHS_VALUE`: Row(1, 1)
///    `LHS_CACHE`: Row(1, 3)
///    `NEW_RHS_VALUE`: Row(1, 2)
///    ---
///    Cache no update, since LHS value would still be largest, nearest to RHS.
///
///    Example B:
///    `RHS_VALUE`: Row(1, 3)
///    `LHS_CACHE`: Row(1, 2)
///    `NEW_RHS_VALUE`: Row(1, 1)
///    ---
///    Cache no update. Largest LHS value that is nearest to RHS remains the same.
///
///    Example C:
///    `RHS_VALUE`: Row(1, 3)
///    `LHS_VALUE`: Row(1, 4)
///    `NEW_RHS_VALUE`: Row(1, 4)
///    ---
///    Cache needs update. Largest LHS value that is nearest to RHS can now be Row(1, 5).
struct DynamicFilterCache {
    value: DynamicFilterCacheEntry,
    /// LHS key column index.
    key_l: usize,
    /// LHS key indices
    pk_indices: Vec<usize>,
}

#[derive(PartialEq)]
enum DynamicFilterCacheEntry {
    /// No values within range (prev_RHS, current_RHS).
    NoMatch,
    /// Nearest value to `prev_RHS` within the range (prev_RHS, current_RHS).
    /// Stores (pk, owned row).
    Match((OwnedRow, OwnedRow)),
    /// No entry, we need initialize it with the first value we receive,
    /// Or do an LHS table scan to refresh it.
    Empty,
}

impl DynamicFilterCache {
    fn new(key_l: usize, pk_indices: Vec<usize>) -> Self {
        Self {
            value: DynamicFilterCacheEntry::Empty,
            key_l,
            pk_indices,
        }
    }

    fn get_cached_lhs(&self) -> Option<&OwnedRow> {
        if let DynamicFilterCacheEntry::Match((_, lhs_row)) = &self.value {
            Some(lhs_row)
        } else {
            None
        }
    }

    /// All rows are guaranteed to be between
    /// (prev_epoch_val, cur_epoch_val].
    /// So we just need to find the minimum
    fn handle_scan_row(&mut self, lhs_row: &OwnedRow) {
        match &self.value {
            DynamicFilterCacheEntry::Empty | DynamicFilterCacheEntry::NoMatch => {
                let lhs_pk = lhs_row.project(self.pk_indices.as_slice()).into_owned_row();
                self.value = DynamicFilterCacheEntry::Match((lhs_pk, lhs_row.clone()));
            }
            DynamicFilterCacheEntry::Match((_, cur_cached_lhs)) => {
                if cmp_datum(
                    lhs_row.datum_at(self.key_l),
                    cur_cached_lhs.datum_at(self.key_l),
                    Default::default(),
                ) == Ordering::Less
                {
                    let lhs_pk = lhs_row.project(self.pk_indices.as_slice()).into_owned_row();
                    self.value = DynamicFilterCacheEntry::Match((lhs_pk, lhs_row.clone()));
                }
            }
        }
    }

    /// If after table scan CacheEntry is still Empty, means no match.
    fn ensure_no_match_if_empty(&mut self) {
        if self.value == DynamicFilterCacheEntry::Empty {
            self.value = DynamicFilterCacheEntry::NoMatch;
        }
    }

    /// `INSERT/UPDATE_INSERT`:
    /// If the new value is (RHS, `old_value`), replace `old_value` in cache.
    /// Otherwise, do nothing.
    ///
    /// `DELETE/UPDATE_DELETE`:
    /// If the new value has the same key as `old_value`, empty cache. It is now `Empty`.
    /// It means for this epoch, we will need to do table scan.
    ///
    /// TODO(kwannoel):
    /// Optimization: For the rest of values in the chunk, if any are between `(RHS, old_value]`,
    /// replace `old_value` in cache.
    fn handle_lhs_row(&mut self, lhs_row: RowRef<'_>, op: &Op, cur_rhs: &Option<Datum>) {
        // IF no rhs value yet, there's nothing to cache.
        let Some(cur_rhs) = cur_rhs else {
            return;
        };
        match self.value {
            DynamicFilterCacheEntry::NoMatch | DynamicFilterCacheEntry::Empty => {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        let new_lhs_value = lhs_row.datum_at(self.key_l);
                        if matches!(
                            cmp_datum(new_lhs_value, cur_rhs, Default::default()),
                            Ordering::Greater
                        ) {
                            let lhs_pk =
                                lhs_row.project(self.pk_indices.as_slice()).into_owned_row();
                            self.value =
                                DynamicFilterCacheEntry::Match((lhs_pk, lhs_row.into_owned_row()));
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        // Nothing needs to be done, cache is empty.
                    }
                }
            }
            DynamicFilterCacheEntry::Match((ref mut old_pk, ref mut old_value)) => {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        let new_lhs_value = lhs_row.datum_at(self.key_l);
                        // RHS < new_lhs_value < old_value
                        if matches!(
                            cmp_datum(new_lhs_value, cur_rhs, Default::default()),
                            Ordering::Greater
                        ) && matches!(
                            cmp_datum(
                                new_lhs_value,
                                old_value.datum_at(self.key_l),
                                Default::default()
                            ),
                            Ordering::Less
                        ) {
                            *old_pk = lhs_row.project(self.pk_indices.as_slice()).into_owned_row();
                            *old_value = lhs_row.into_owned_row();
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        let new_lhs_pk = lhs_row.project(self.pk_indices.as_slice());
                        if Row::eq(old_pk, new_lhs_pk) {
                            self.value = DynamicFilterCacheEntry::Empty;
                        }
                    }
                }
            }
        }
    }

    fn handle_rhs_row(&mut self, op: &Op, prev_rhs: &Option<Datum>, new_rhs: DatumRef<'_>) {
        let Some(prev_rhs) = prev_rhs else {
            return;
        };
        match self.value {
            DynamicFilterCacheEntry::Empty => {}
            DynamicFilterCacheEntry::NoMatch => {
                match op {
                    // Guaranteed it is monotonically increasing,
                    // so means if no match now, no match later either,
                    // since there's no LHS value larger currently.
                    // If we update delete, it has to be paired with an
                    // UpdateInsert with a larger value later too.
                    Op::Insert | Op::UpdateInsert | Op::UpdateDelete => {}

                    // If just delete, it's possible that we have no RHS value left.
                    // OR we revert to an older value, i.e. smaller.
                    // There could be a match in that case.
                    // So we need to mark as empty, so we can refresh it later.
                    Op::Delete => self.value = DynamicFilterCacheEntry::Empty,
                }
            }
            DynamicFilterCacheEntry::Match((ref mut _cur_pk, ref mut cur_value)) => {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        // cur_RHS <= new_RHS < cur_value => Can ignore safely.
                        // otherwise RHS less or more => Empty cache, refresh it on table scan.
                        if matches!(
                            cmp_datum(new_rhs, prev_rhs, Default::default()),
                            Ordering::Greater | Ordering::Equal
                        ) && matches!(
                            cmp_datum(new_rhs, cur_value.datum_at(self.key_l), Default::default()),
                            Ordering::Less
                        ) {
                            // No need to do anything.
                        } else {
                            self.value = DynamicFilterCacheEntry::Empty
                        }
                    }
                    Op::UpdateDelete => {
                        // Depends on `Op::UpdateInsert`. Can ignore it.
                    }
                    Op::Delete => self.value = DynamicFilterCacheEntry::Empty,
                }
            }
        }
    }
}

pub struct DynamicFilterExecutor<S: StateStore> {
    ctx: ActorContextRef,
    source_l: Option<BoxedExecutor>,
    source_r: Option<BoxedExecutor>,
    key_l: usize,
    pk_indices: PkIndices,
    identity: String,
    comparator: ExprNodeType,
    left_table: StateTable<S>,
    right_table: StateTable<S>,
    schema: Schema,
    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
}

impl<S: StateStore> DynamicFilterExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        source_l: BoxedExecutor,
        source_r: BoxedExecutor,
        key_l: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        comparator: ExprNodeType,
        state_table_l: StateTable<S>,
        state_table_r: StateTable<S>,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> Self {
        let schema = source_l.schema().clone();
        Self {
            ctx,
            source_l: Some(source_l),
            source_r: Some(source_r),
            key_l,
            pk_indices,
            identity: format!("DynamicFilterExecutor {:X}", executor_id),
            comparator,
            left_table: state_table_l,
            right_table: state_table_r,
            metrics,
            schema,
            chunk_size,
        }
    }

    async fn apply_batch(
        &mut self,
        data_chunk: &DataChunk,
        ops: Vec<Op>,
        condition: Option<BoxedExpression>,
        cache: &mut DynamicFilterCache,
        cur_rhs: &Option<Datum>,
    ) -> Result<(Vec<Op>, Bitmap), StreamExecutorError> {
        debug_assert_eq!(ops.len(), data_chunk.cardinality());
        let mut new_ops = Vec::with_capacity(ops.len());
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        let mut last_res = false;

        let eval_results = if let Some(cond) = condition {
            Some(
                cond.eval_infallible(data_chunk, |err| {
                    self.ctx.on_compute_error(err, &self.identity)
                })
                .await,
            )
        } else {
            None
        };

        for (idx, (row, op)) in data_chunk.rows().zip_eq_debug(ops.iter()).enumerate() {
            let left_val = row.datum_at(self.key_l).to_owned_datum();

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
            if left_val.is_some() {
                match *op {
                    Op::Insert | Op::UpdateInsert => {
                        self.left_table.insert(row);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.left_table.delete(row);
                    }
                }
                cache.handle_lhs_row(row, op, cur_rhs)
            }
        }

        let new_visibility = new_visibility.finish();

        Ok((new_ops, new_visibility))
    }

    /// Returns the required range, whether the latest value is in lower bound (rather than upper)
    /// and whether to insert or delete the range.
    fn get_range(
        &self,
        curr: &Datum,
        prev: Datum,
    ) -> ((Bound<ScalarImpl>, Bound<ScalarImpl>), bool, bool) {
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
                // The new bound is always towards the last known value
                let is_lower = matches!(self.comparator, GreaterThan | GreaterThanOrEqual);
                (range, is_lower, is_insert)
            }
            (Some(c), Some(p)) => {
                if c.default_cmp(&p).is_lt() {
                    let range = match self.comparator {
                        GreaterThan | LessThanOrEqual => (Excluded(c), Included(p)),
                        GreaterThanOrEqual | LessThan => (Included(c), Excluded(p)),
                        _ => unreachable!(),
                    };
                    let is_insert = matches!(self.comparator, GreaterThan | GreaterThanOrEqual);
                    (range, true, is_insert)
                } else {
                    // c > p
                    let range = match self.comparator {
                        GreaterThan | LessThanOrEqual => (Excluded(p), Included(c)),
                        GreaterThanOrEqual | LessThan => (Included(p), Excluded(c)),
                        _ => unreachable!(),
                    };
                    let is_insert = matches!(self.comparator, LessThan | LessThanOrEqual);
                    (range, false, is_insert)
                }
            }
            (None, None) => unreachable!(), // prev != curr
        }
    }

    async fn recover_rhs(&mut self) -> Result<Option<RowData>, StreamExecutorError> {
        // Recover value for RHS if available
        let rhs_stream = self.right_table.iter(Default::default()).await?;
        pin_mut!(rhs_stream);

        if let Some(res) = rhs_stream.next().await {
            let value = res?;
            assert!(rhs_stream.next().await.is_none());
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn to_row_bound(bound: Bound<ScalarImpl>) -> Bound<impl Row> {
        bound.map(|s| once(Some(s)))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let input_l = self.source_l.take().unwrap();
        let input_r = self.source_r.take().unwrap();

        let left_len = input_l.schema().len();

        let mut cache = DynamicFilterCache::new(self.key_l, self.pk_indices.clone());

        // Derive the dynamic expression
        let l_data_type = input_l.schema().data_types()[self.key_l].clone();
        let r_data_type = input_r.schema().data_types()[0].clone();
        // The types are aligned by frontend.
        assert_eq!(l_data_type, r_data_type);
        let dynamic_cond = move |literal: Datum| {
            literal.map(|scalar| {
                build_func(
                    self.comparator,
                    DataType::Boolean,
                    vec![
                        Box::new(InputRefExpression::new(l_data_type.clone(), self.key_l)),
                        Box::new(LiteralExpression::new(r_data_type.clone(), Some(scalar))),
                    ],
                )
            })
        };

        let aligned_stream = barrier_align(
            input_l.execute(),
            input_r.execute(),
            self.ctx.id,
            self.metrics.clone(),
        );

        pin_mut!(aligned_stream);

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        self.right_table.init_epoch(barrier.epoch);
        self.left_table.init_epoch(barrier.epoch);

        let recovered_row = self.recover_rhs().await?;
        let recovered_value = recovered_row.as_ref().map(|r| r[0].clone());
        // At the beginning of an epoch, the `prev_epoch_value` == `current_epoch_value`
        let mut prev_epoch_value: Option<Datum> = recovered_value.clone();
        let mut current_epoch_value: Option<Datum> = recovered_value;
        // This is only required to be some if the row arrived during this epoch.
        let mut current_epoch_row = recovered_row.clone();
        let mut last_committed_epoch_row = recovered_row;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        let (left_to_output, _) =
            StreamChunkBuilder::get_i2o_mapping(0..self.schema.len(), left_len, 0);
        let mut stream_chunk_builder = StreamChunkBuilder::new(
            self.chunk_size,
            &self.schema.data_types(),
            vec![],
            left_to_output,
        );

        let watermark_can_clean_state = !matches!(self.comparator, LessThan | LessThanOrEqual);
        let mut unused_clean_hint = None;

        #[for_await]
        for msg in aligned_stream {
            match msg? {
                AlignedMessage::Left(chunk) => {
                    // Reuse the logic from `FilterExecutor`
                    let chunk = chunk.compact(); // Is this unnecessary work?
                    let (data_chunk, ops) = chunk.into_parts();

                    let right_val = prev_epoch_value.clone().flatten();

                    // The condition is `None` if it is always false by virtue of a NULL right
                    // input, so we save evaluating it on the datachunk
                    let condition = dynamic_cond(right_val).transpose()?;

                    let (new_ops, new_visibility) = self
                        .apply_batch(
                            &data_chunk,
                            ops,
                            condition,
                            &mut cache,
                            &current_epoch_value,
                        )
                        .await?;

                    let (columns, _) = data_chunk.into_parts();

                    if new_visibility.count_ones() > 0 {
                        let new_chunk = StreamChunk::new(new_ops, columns, Some(new_visibility));
                        yield Message::Chunk(new_chunk)
                    }
                }
                AlignedMessage::Right(chunk) => {
                    // Record the latest update to the right value
                    let chunk = chunk.compact(); // Is this unnecessary work?
                    let (data_chunk, ops) = chunk.into_parts();

                    for (row, op) in data_chunk.rows().zip_eq_debug(ops.iter()) {
                        cache.handle_rhs_row(op, &current_epoch_value, row.datum_at(0));
                        match *op {
                            Op::UpdateInsert | Op::Insert => {
                                current_epoch_value = Some(row.datum_at(0).to_owned_datum());
                                current_epoch_row = Some(row.into_owned_row());
                            }
                            _ => {
                                // To be consistent, there must be an existing `current_epoch_value`
                                // equivalent to row indicated for
                                // deletion.
                                if Some(row.datum_at(0))
                                    != current_epoch_value.as_ref().map(ToDatumRef::to_datum_ref)
                                {
                                    bail!(
                                        "Inconsistent Delete - current: {:?}, delete: {:?}",
                                        current_epoch_value,
                                        row
                                    );
                                }
                                current_epoch_value = None;
                                current_epoch_row = None;
                            }
                        }
                    }
                }
                AlignedMessage::WatermarkLeft(_) => {
                    // Do nothing.
                }
                AlignedMessage::WatermarkRight(watermark) => {
                    if watermark_can_clean_state {
                        unused_clean_hint = Some(watermark);
                    }
                }
                AlignedMessage::Barrier(barrier) => {
                    // Flush the difference between the `prev_value` and `current_value`
                    //
                    // This block is guaranteed to be idempotent even if we may encounter multiple
                    // barriers since `prev_epoch_value` is always be reset to
                    // the equivalent of `current_epoch_value` at the end of
                    // this block. Likewise, `last_committed_epoch_row` will always be equal to
                    // `current_epoch_row`.
                    // It is thus guaranteed not to commit state or produce chunks as long as
                    // no new chunks have arrived since the previous barrier.
                    let curr: Datum = current_epoch_value.clone().flatten();
                    let prev: Datum = prev_epoch_value.flatten();
                    if prev != curr {
                        let (range, _latest_is_lower, is_insert) = self.get_range(&curr, prev);
                        let left_bound = if let Some(lhs) = cache.get_cached_lhs()
                            && let Some(lhs_value) = lhs.datum_at(self.key_l) // FIXME should only cache scalar.
                        {
                            Bound::Included(lhs_value.into_scalar_impl())
                        } else {
                            range.0
                        };
                        let range = (Self::to_row_bound(left_bound), Self::to_row_bound(range.1));

                        // TODO: prefetching for append-only case.
                        let streams = futures::future::try_join_all(
                            self.left_table.vnodes().iter_vnodes().map(|vnode| {
                                self.left_table.iter_with_pk_range(
                                    &range,
                                    vnode,
                                    PrefetchOptions::new_for_exhaust_iter(),
                                )
                            }),
                        )
                        .await?
                        .into_iter()
                        .map(Box::pin);

                        #[for_await]
                        for res in stream::select_all(streams) {
                            let row = res?;
                            cache.handle_scan_row(&row);
                            if let Some(chunk) = stream_chunk_builder.append_row_matched(
                                // All rows have a single identity at this point
                                if is_insert { Op::Insert } else { Op::Delete },
                                row,
                            ) {
                                yield Message::Chunk(chunk);
                            }
                        }

                        cache.ensure_no_match_if_empty();

                        if let Some(chunk) = stream_chunk_builder.take() {
                            yield Message::Chunk(chunk);
                        }
                    }

                    if let Some(mut watermark) = unused_clean_hint.take() {
                        self.left_table
                            .update_watermark(watermark.val.clone(), false);
                        watermark.col_idx = self.key_l;
                        yield Message::Watermark(watermark);
                    };

                    // Update the committed value on RHS if it has changed.
                    if last_committed_epoch_row != current_epoch_row {
                        // Only write the RHS value if this actor is in charge of vnode 0 on LHS
                        // Otherwise, we only actively replicate the changes.
                        if self.left_table.vnode_bitmap().is_set(0) {
                            // If both `None`, then this branch is inactive.
                            // Hence, at least one is `Some`, hence at least one update.
                            if let Some(old_row) = last_committed_epoch_row.take() {
                                self.right_table.delete(old_row);
                            }
                            if let Some(row) = &current_epoch_row {
                                self.right_table.insert(row);
                            }
                            self.right_table.commit(barrier.epoch).await?;
                        } else {
                            self.right_table.commit_no_data_expected(barrier.epoch);
                        }
                        // Update the last committed row since it has changed
                        last_committed_epoch_row = current_epoch_row.clone();
                    } else {
                        self.right_table.commit_no_data_expected(barrier.epoch);
                    }

                    self.left_table.commit(barrier.epoch).await?;

                    prev_epoch_value = Some(curr);

                    debug_assert_eq!(last_committed_epoch_row, current_epoch_row);

                    // Update the vnode bitmap for the left state table if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let (_previous_vnode_bitmap, _cache_may_stale) =
                            self.left_table.update_vnode_bitmap(vnode_bitmap);
                    }

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

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, StreamExecutorResult};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        let column_descs = ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64);
        // TODO: use consistent operations for dynamic filter <https://github.com/risingwavelabs/risingwave/issues/3893>
        let state_table_l = StateTable::new_without_distribution_inconsistent_op(
            mem_state.clone(),
            TableId::new(0),
            vec![column_descs.clone()],
            vec![OrderType::ascending()],
            vec![0],
        )
        .await;
        let state_table_r = StateTable::new_without_distribution_inconsistent_op(
            mem_state,
            TableId::new(1),
            vec![column_descs],
            vec![OrderType::ascending()],
            vec![0],
        )
        .await;
        (state_table_l, state_table_r)
    }

    async fn create_executor(
        comparator: ExprNodeType,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let mem_state = MemoryStateStore::new();
        create_executor_inner(comparator, mem_state).await
    }

    async fn create_executor_inner(
        comparator: ExprNodeType,
        mem_state: MemoryStateStore,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let (mem_state_l, mem_state_r) = create_in_memory_state_table(mem_state).await;
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (tx_l, source_l) = MockSource::channel(schema.clone(), vec![0]);
        let (tx_r, source_r) = MockSource::channel(schema, vec![]);

        let executor = DynamicFilterExecutor::<MemoryStateStore>::new(
            ActorContext::create(123),
            Box::new(source_l),
            Box::new(source_r),
            0,
            vec![0],
            1,
            comparator,
            mem_state_l,
            mem_state_r,
            Arc::new(StreamingMetrics::unused()),
            1024,
        );
        (tx_l, tx_r, Box::new(executor).execute())
    }

    #[tokio::test]
    async fn test_dynamic_filter_rhs_recovery_gt() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 4
             - 3",
        );
        let chunk_r0 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             - 1
             + 2",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 4",
        );
        let mem_state = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor_inner(ExprNodeType::GreaterThan, mem_state.clone()).await;

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 0th right chunk
        tx_r.push_chunk(chunk_r0);

        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // Drop executor corresponding to node failure
        drop(tx_l);
        drop(tx_r);
        drop(dynamic_filter);

        // Recover executor from state store
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor_inner(ExprNodeType::GreaterThan, mem_state.clone()).await;

        // push the recovery barrier for left and right
        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);

        // Get recovery barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the init barrier for left and right
        tx_l.push_barrier(3, false);
        tx_r.push_barrier(3, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2
                + 3"
            )
        );

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // Drop executor corresponding to node failure
        drop(tx_l);
        drop(tx_r);
        drop(dynamic_filter);

        // Recover executor from state store
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor_inner(ExprNodeType::GreaterThan, mem_state.clone()).await;

        // push recovery barrier
        tx_l.push_barrier(3, false);
        tx_r.push_barrier(3, false);

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                - 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(4, false);
        tx_r.push_barrier(4, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(5, false);
        tx_r.push_barrier(5, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2
                - 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_greater_than() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 4
             - 3",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 4",
        );
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(ExprNodeType::GreaterThan).await;

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                - 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(3, false);
        tx_r.push_barrier(3, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(4, false);
        tx_r.push_barrier(4, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2
                - 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_greater_than_or_equal() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 4
             - 3",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 5",
        );
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(ExprNodeType::GreaterThanOrEqual).await;

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                - 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(3, false);
        tx_r.push_barrier(3, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(4, false);
        tx_r.push_barrier(4, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2
                - 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_less_than() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 2
             + 3
             + 4",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 1
             - 2",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 4",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(ExprNodeType::LessThan).await;

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 1
                - 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(3, false);
        tx_r.push_barrier(3, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(4, false);
        tx_r.push_barrier(4, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 1
                - 3"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_less_than_or_equal() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 2
             + 3
             + 4",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 1
             - 2",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 3",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 0",
        );
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(ExprNodeType::LessThanOrEqual).await;

        // push the init barrier for left and right
        tx_l.push_barrier(1, false);
        tx_r.push_barrier(1, false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(2, false);
        tx_r.push_barrier(2, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 1
                - 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(3, false);
        tx_r.push_barrier(3, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(4, false);
        tx_r.push_barrier(4, false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 1
                - 3"
            )
        );

        Ok(())
    }
}
