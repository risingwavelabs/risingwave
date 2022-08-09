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

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{pin_mut, StreamExt};
use futures_async_stream::for_await;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::{Array, ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::*;
use risingwave_common::util::sort_util::{DescOrderedRow, OrderPair, OrderType};
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::state_table::RowBasedStateTable;
use risingwave_storage::StateStore;

use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::{AggArgs, AggCall};
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::iter_state_table;
use crate::executor::PkIndices;

pub type ManagedMinState<S, A> = GenericExtremeState<S, A, { variants::EXTREME_MIN }>;
pub type ManagedMaxState<S, A> = GenericExtremeState<S, A, { variants::EXTREME_MAX }>;

/// All possible extreme types.
pub mod variants {
    pub const EXTREME_MIN: usize = 0;
    pub const EXTREME_MAX: usize = 1;
}

#[derive(Debug)]
struct Cache {
    synced: bool,            // `false` means not synced with state table (cold start)
    capacity: Option<usize>, // `None` means unlimited capacity
    order_pairs: Arc<Vec<OrderPair>>, // order requirements used to sort cached rows
    rows: BTreeSet<DescOrderedRow>, // in reverse order of `order_pairs`
}

impl Cache {
    fn new(capacity: Option<usize>, order_pairs: Vec<OrderPair>) -> Self {
        Self {
            synced: false,
            capacity,
            order_pairs: Arc::new(order_pairs),
            rows: BTreeSet::new(),
        }
    }

    fn set_synced(&mut self) {
        self.synced = true;
    }

    fn insert(&mut self, row: Row) {
        if self.synced {
            let ordered_row = DescOrderedRow::new(row, None, self.order_pairs.clone());
            self.rows.insert(ordered_row);
            // evict if capacity is reached
            if let Some(capacity) = self.capacity {
                while self.rows.len() > capacity {
                    self.rows.pop_first();
                }
            }
        }
    }

    fn remove(&mut self, row: Row) {
        if self.synced {
            let ordered_row = DescOrderedRow::new(row, None, self.order_pairs.clone());
            self.rows.remove(&ordered_row);
        }
    }

    fn first(&self) -> Option<&Row> {
        if self.synced {
            self.rows.last().map(|row| &row.row)
        } else {
            None
        }
    }
}

/// Manages a `BTreeMap` in memory for top N entries, and the state store for remaining entries.
///
/// There are several prerequisites for using the `MinState`.
/// * Sort key must be unique. Users should always encode the sort key as value + row id. The
///   current interface doesn't support this, and we should add support in the next refactor. The
///   `K: Scalar` trait bound should be something like `SortKeySerializer`, and what actually stored
///   in the `BTreeMap` should be `(Datum, RowId)`.
/// * The order encoded key and the `Ord` implementation of the sort key must be the same. If they
///   are different, it is more likely a bug of the encoder, and should be fixed.
/// * Key can be null.
///
/// In the state store, the datum is stored as `sort_key -> encoded_value`. Therefore, we should
/// have the following relationship:
/// * `sort_key = sort_key_encode(key)`
/// * `encoded_value = datum_encode(key)`
/// * `key = datum_decode(encoded_value)`
///
/// The `ExtremeState` need some special properties from the storage engine and the executor.
/// * The output of an `ExtremeState` is only correct when all changes have been flushed to the
///   state store.
/// * The `RowIDs` must be i64
pub struct GenericExtremeState<S: StateStore, A: Array, const EXTREME_TYPE: usize>
where
    A::OwnedItem: Ord,
{
    // TODO(rc): Remove the phantoms.
    _phantom_data: PhantomData<S>,
    _phantom_data_2: PhantomData<A>,

    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// Contains the column mapping between upstream schema and state table.
    state_table_col_mapping: Arc<StateTableColumnMapping>,

    /// The column to aggregate in state table.
    state_table_agg_col_idx: usize,

    /// Number of items in the state including those not in top n cache but in state store.
    total_count: usize, // TODO(rc): is this really needed?

    /// Cache for top N elements in the state.
    cache: Cache,
}

/// A trait over all table-structured states.
///
/// It is true that this interface also fits to value managed state, but we won't implement
/// `ManagedTableState` for them. We want to reduce the overhead of `BoxedFuture`. For
/// `ManagedValueState`, we can directly forward its async functions to `ManagedStateImpl`, instead
/// of adding a layer of indirection caused by async traits.
#[async_trait]
pub trait ManagedTableState<S: StateStore>: Send + Sync + 'static {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
        epoch: u64,
        state_table: &mut RowBasedStateTable<S>,
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Datum>;

    /// Check if this state needs a flush.
    fn is_dirty(&self) -> bool;

    /// Flush the internal state to a write batch.
    fn flush(&mut self, state_table: &mut RowBasedStateTable<S>) -> StreamExecutorResult<()>;
}

// TODO(rc): kill EXTREME_TYPE and A
impl<S: StateStore, A: Array, const EXTREME_TYPE: usize> GenericExtremeState<S, A, EXTREME_TYPE>
where
    A::OwnedItem: Ord,
    for<'a> &'a A: From<&'a ArrayImpl>,
{
    /// Create a managed min state. When `top_n_count` is `None`, the cache will
    /// always be retained when flushing the managed state. Otherwise, we will only retain n entries
    /// after each flush.
    pub fn new(
        agg_call: AggCall,
        group_key: Option<&Row>,
        pk_indices: PkIndices,
        col_mapping: Arc<StateTableColumnMapping>,
        row_count: usize,
        cache_capacity: Option<usize>,
    ) -> StreamExecutorResult<Self> {
        // map agg column to state table column index
        let state_table_agg_col_idx = col_mapping
            .upstream_to_state_table(agg_call.args.val_indices()[0])
            .expect("the column to be aggregate must appear in the state table");
        // map order by columns to state table column indices
        let order_pairs = [OrderPair::new(
            state_table_agg_col_idx,
            match agg_call.kind {
                AggKind::Min => OrderType::Ascending,
                AggKind::Max => OrderType::Descending,
                _ => unreachable!(),
            },
        )]
        .into_iter()
        .chain(pk_indices.iter().map(|idx| {
            OrderPair::new(
                col_mapping
                    .upstream_to_state_table(*idx)
                    .expect("the pk columns must appear in the state table"),
                OrderType::Ascending,
            )
        }))
        .collect();

        Ok(Self {
            _phantom_data: PhantomData,
            _phantom_data_2: PhantomData,
            group_key: group_key.cloned(),
            state_table_col_mapping: col_mapping,
            state_table_agg_col_idx,
            total_count: row_count,
            cache: Cache::new(cache_capacity, order_pairs),
        })
    }

    /// Apply a batch of data to the state.
    fn apply_batch_inner(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        chunk_cols: &[&ArrayImpl], // contains all upstream columns
        state_table: &mut RowBasedStateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, chunk_cols));

        for (i, op) in ops.iter().enumerate() {
            let visible = visibility.map(|x| x.is_set(i).unwrap()).unwrap_or(true);
            if !visible {
                continue;
            }

            let state_row = Row::new(
                self.state_table_col_mapping
                    .upstream_columns()
                    .iter()
                    .map(|col_idx| chunk_cols[*col_idx].datum_at(i))
                    .collect(),
            );

            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.cache.insert(state_row.clone());
                    state_table.insert(state_row)?;
                    self.total_count += 1;
                }
                Op::Delete | Op::UpdateDelete => {
                    self.cache.remove(state_row.clone());
                    state_table.delete(state_row)?;
                    self.total_count -= 1;
                }
            }
        }

        Ok(())
    }

    // TODO(rc): This is the only part that differs between different agg states.
    // So we may introduce a trait for this later, and other code can be reused
    // for each type of state.
    fn get_output_from_cache(&self) -> Option<Datum> {
        let row = self.cache.first()?;
        Some(row[self.state_table_agg_col_idx].clone())
    }

    async fn get_output_inner(
        &mut self,
        epoch: u64,
        state_table: &RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        // try to get the result from cache
        if let Some(datum) = self.get_output_from_cache() {
            Ok(datum)
        } else {
            // read from state table and fill in the cache
            let all_data_iter =
                iter_state_table(state_table, epoch, self.group_key.as_ref()).await?;
            pin_mut!(all_data_iter);

            self.cache.set_synced(); // after the following loop the cache should be synced

            #[for_await]
            for state_row in all_data_iter.take(self.cache.capacity.unwrap_or(usize::MAX)) {
                let state_row = state_row?;
                self.cache.insert(state_row.as_ref().to_owned());
            }

            // try to get the result from cache again
            if let Some(datum) = self.get_output_from_cache() {
                Ok(datum)
            } else {
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl<S: StateStore, A: Array, const EXTREME_TYPE: usize> ManagedTableState<S>
    for GenericExtremeState<S, A, EXTREME_TYPE>
where
    A::OwnedItem: Ord,
    for<'a> &'a A: From<&'a ArrayImpl>,
{
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
        _epoch: u64,
        state_table: &mut RowBasedStateTable<S>,
    ) -> StreamExecutorResult<()> {
        self.apply_batch_inner(ops, visibility, data, state_table)
    }

    async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        self.get_output_inner(epoch, state_table).await
    }

    /// Check if this state needs a flush.
    /// TODO: Remove this.
    fn is_dirty(&self) -> bool {
        unreachable!("Should not call this function anymore, check state table for dirty data");
    }

    fn flush(&mut self, _state_table: &mut RowBasedStateTable<S>) -> StreamExecutorResult<()> {
        Ok(())
    }
}

pub fn create_streaming_extreme_state<S: StateStore>(
    agg_call: AggCall,
    group_key: Option<&Row>,
    pk_indices: PkIndices,
    col_mapping: Arc<StateTableColumnMapping>,
    row_count: usize,
    cache_capacity: Option<usize>,
) -> StreamExecutorResult<Box<dyn ManagedTableState<S>>> {
    match &agg_call.args {
        AggArgs::Unary(x, _) => {
            if agg_call.return_type != *x {
                panic!(
                    "extreme state input doesn't match return value: {:?}",
                    agg_call
                );
            }
        }
        _ => panic!("extreme state should only have one arg: {:?}", agg_call),
    }

    macro_rules! match_new_extreme_state {
        ($( { $( $kind:pat_param )|+, $array:ty } ),* $(,)?) => {{
            use DataType::*;
            use risingwave_common::array::*;

            match (agg_call.kind.clone(), agg_call.return_type.clone()) {
                // TODO(rc): this two can be merged
                $(
                    (AggKind::Max, $( $kind )|+) => Ok(Box::new(
                        ManagedMaxState::<_, $array>::new(
                            agg_call,
                            group_key,
                            pk_indices,
                            col_mapping,
                            row_count,
                            cache_capacity,
                        )?,
                    )),
                    (AggKind::Min, $( $kind )|+) => Ok(Box::new(
                        ManagedMinState::<_, $array>::new(
                            agg_call,
                            group_key,
                            pk_indices,
                            col_mapping,
                            row_count,
                            cache_capacity,
                        )?,
                    )),
                )*
                (kind, return_type) => unimplemented!("unsupported extreme agg, kind: {:?}, return type: {:?}", kind, return_type),
            }
        }};
    }

    match_new_extreme_state!(
        { Boolean, BoolArray },
        { Int64, I64Array },
        { Int32, I32Array },
        { Int16, I16Array },
        { Float64, F64Array },
        { Float32, F32Array },
        { Decimal, DecimalArray },
        { Date, NaiveDateArray },
        { Varchar, Utf8Array },
        { Time, NaiveTimeArray },
        { Timestamp, NaiveDateTimeArray },
        { Interval, IntervalArray },
        { Struct { fields: _ }, StructArray },
        { List { datatype: _ }, ListArray },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use smallvec::smallvec;

    use super::*;

    #[tokio::test]
    async fn test_managed_extreme_state() {
        let store = MemoryStateStore::new();
        let mut state_table =
            state_table_create_helper(store, TableId::from(0x2333), 1, OrderType::Ascending);

        let mut managed_state = ManagedMinState::<_, I64Array>::new(
            Some(5),
            0,
            PkDataTypes::new(),
            HashCode(567),
            None,
        )
        .unwrap();
        assert!(!state_table.is_dirty());

        let mut epoch: u64 = 0;
        // insert 0, 10, 20
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(10), Some(20)])
                    .unwrap()
                    .into()],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();
        assert!(state_table.is_dirty());

        // flush to write batch and write to state store
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be 0
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(0))
        );

        // insert 5, 15, 25, and do a flush. After that, `25`, `27, and `30` should be evicted from
        // the top n cache.
        managed_state
            .apply_batch(
                &[Op::Insert; 5],
                None,
                &[
                    &I64Array::from_slice(&[Some(5), Some(15), Some(25), Some(27), Some(30)])
                        .unwrap()
                        .into(),
                ],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        epoch += 1;
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be 0
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(0))
        );

        // delete 0, 10, 5, 15, so that the top n cache now contains only 20.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
                None,
                &[
                    &I64Array::from_slice(&[Some(0), Some(10), Some(15), Some(5)])
                        .unwrap()
                        .into(),
                ],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        epoch += 1;
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be 20
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(20))
        );

        // delete 20, 27, so that the top n cache is empty, and we should get minimum 25.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Delete],
                None,
                &[&I64Array::from_slice(&[Some(20), Some(27)]).unwrap().into()],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        epoch += 1;
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be 25
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(25))
        );

        // delete 25, and we should get minimum 30.
        managed_state
            .apply_batch(
                &[Op::Delete],
                None,
                &[&I64Array::from_slice(&[Some(25)]).unwrap().into()],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        epoch += 1;
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be 30
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(30))
        );

        let row_count = managed_state.total_count;

        // test recovery
        let mut managed_state = ManagedMinState::<_, I64Array>::new(
            Some(5),
            row_count,
            PkDataTypes::new(),
            HashCode(567),
            None,
        )
        .unwrap();

        // The minimum should still be 30
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(30))
        );
    }

    #[tokio::test]
    async fn test_replicated_value_min() {
        test_replicated_value_not_null::<{ variants::EXTREME_MIN }>().await
    }

    #[tokio::test]
    async fn test_replicated_value_max() {
        test_replicated_value_not_null::<{ variants::EXTREME_MAX }>().await
    }

    #[tokio::test]
    async fn test_replicated_value_min_with_null() {
        test_replicated_value_with_null::<{ variants::EXTREME_MIN }>().await
    }

    #[tokio::test]
    async fn test_replicated_value_max_with_null() {
        test_replicated_value_with_null::<{ variants::EXTREME_MAX }>().await
    }

    fn state_table_create_helper<S: StateStore>(
        store: S,
        table_id: TableId,
        column_cnt: usize,
        order_type: OrderType,
    ) -> RowBasedStateTable<S> {
        let mut column_descs = Vec::with_capacity(column_cnt);
        for id in 0..column_cnt {
            column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(id as i32),
                DataType::Int64,
            ));
        }
        let relational_pk_len = column_descs.len();
        RowBasedStateTable::new_without_distribution(
            store,
            table_id,
            column_descs,
            vec![order_type; relational_pk_len],
            (0..relational_pk_len).collect(),
        )
    }

    async fn test_replicated_value_not_null<const EXTREME_TYPE: usize>() {
        let store = MemoryStateStore::new();
        let order_type = if EXTREME_TYPE == variants::EXTREME_MAX {
            OrderType::Descending
        } else {
            OrderType::Ascending
        };
        let mut state_table =
            state_table_create_helper(store, TableId::from(0x2333), 2, order_type);

        let mut managed_state = GenericExtremeState::<_, I64Array, EXTREME_TYPE>::new(
            Some(3),
            0,
            smallvec![DataType::Int64],
            HashCode(567),
            None,
        )
        .unwrap();
        assert!(!state_table.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(1), Some(1), Some(4), Some(5), Some(1), Some(4)])
                .unwrap()
                .into();

        let pk_buffer = I64Array::from_slice(&[
            Some(1001),
            Some(1002),
            Some(1003),
            Some(1004),
            Some(1005),
            Some(1006),
        ])
        .unwrap()
        .into();

        let extreme = match EXTREME_TYPE {
            variants::EXTREME_MIN => Some(ScalarImpl::Int64(1)),
            variants::EXTREME_MAX => Some(ScalarImpl::Int64(5)),
            _ => unreachable!(),
        };

        let mut epoch = 0;

        // insert 1, 5
        managed_state
            .apply_batch(
                &[Op::Insert; 2],
                None,
                &[
                    &I64Array::from_slice(&[Some(1), Some(5)]).unwrap().into(),
                    &I64Array::from_slice(&[Some(2001), Some(2002)])
                        .unwrap()
                        .into(),
                ],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // insert 1 1 4 5 1 4
        managed_state
            .apply_batch(
                &[Op::Insert; 6],
                None,
                &[&value_buffer, &pk_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be 1, or the maximum should be 5
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            extreme
        );

        // delete 1 1 4 5 1 4
        managed_state
            .apply_batch(
                &[Op::Delete; 6],
                None,
                &[&value_buffer, &pk_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush
        epoch += 1;
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should still be 1, or the maximum should still be 5
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            extreme
        );
    }

    async fn test_replicated_value_with_null<const EXTREME_TYPE: usize>() {
        let store = MemoryStateStore::new();
        let order_type = if EXTREME_TYPE == variants::EXTREME_MAX {
            OrderType::Descending
        } else {
            OrderType::Ascending
        };
        let mut state_table =
            state_table_create_helper(store, TableId::from(0x2333), 2, order_type);

        let mut managed_state = GenericExtremeState::<_, I64Array, EXTREME_TYPE>::new(
            Some(3),
            0,
            smallvec![DataType::Int64],
            HashCode(567),
            None,
        )
        .unwrap();
        assert!(!state_table.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(1), Some(1), Some(4), Some(5), Some(1), Some(4)])
                .unwrap()
                .into();

        let pk_buffer = I64Array::from_slice(&[
            Some(1001),
            Some(1002),
            Some(1003),
            Some(1004),
            Some(1005),
            Some(1006),
        ])
        .unwrap()
        .into();

        let extreme = match EXTREME_TYPE {
            variants::EXTREME_MIN => None,
            variants::EXTREME_MAX => Some(ScalarImpl::Int64(5)),
            _ => unreachable!(),
        };

        let mut epoch = 0;

        // insert 1, 5
        managed_state
            .apply_batch(
                &[Op::Insert; 2],
                None,
                &[
                    &I64Array::from_slice(&[Some(1), Some(5)]).unwrap().into(),
                    &I64Array::from_slice(&[Some(2001), Some(2002)])
                        .unwrap()
                        .into(),
                ],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        let null_buffer = &I64Array::from_slice(&[None, None, None]).unwrap().into();
        let null_pk_buffer = &I64Array::from_slice(&[Some(3001), Some(3002), Some(3003)])
            .unwrap()
            .into();

        // insert None None None
        managed_state
            .apply_batch(
                &[Op::Insert; 3],
                None,
                &[null_buffer, null_pk_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // insert 1 1 4 5 1 4
        managed_state
            .apply_batch(
                &[Op::Insert; 6],
                None,
                &[&value_buffer, &pk_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should be None, or the maximum should be 5
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            extreme
        );

        // delete 1 1 4 5 1 4
        managed_state
            .apply_batch(
                &[Op::Delete; 6],
                None,
                &[&value_buffer, &pk_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // flush
        epoch += 1;
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // The minimum should still be None, or the maximum should still be 5
        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            extreme
        );
    }

    #[tokio::test]
    async fn test_same_group_of_value() {
        let store = MemoryStateStore::new();
        let mut managed_state = ManagedMinState::<_, I64Array>::new(
            Some(3),
            0,
            PkDataTypes::new(),
            HashCode(567),
            None,
        )
        .unwrap();
        let mut state_table =
            state_table_create_helper(store, TableId::from(0x2333), 1, OrderType::Ascending);

        assert!(!state_table.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)])
                .unwrap()
                .into();
        let mut epoch: u64 = 0;

        managed_state
            .apply_batch(
                &[Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(6)]).unwrap().into()],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        for i in 0..100 {
            managed_state
                .apply_batch(
                    &[Op::Insert; 6],
                    None,
                    &[&value_buffer],
                    epoch,
                    &mut state_table,
                )
                .await
                .unwrap();

            if i % 2 == 0 {
                // only ingest after insert in some cases
                // flush to write batch and write to state store
                epoch += 1;
                helper_flush(&mut managed_state, epoch, &mut state_table).await;
            }

            managed_state
                .apply_batch(
                    &[Op::Delete; 6],
                    None,
                    &[&value_buffer],
                    epoch,
                    &mut state_table,
                )
                .await
                .unwrap();

            // flush to write batch and write to state store
            epoch += 1;
            helper_flush(&mut managed_state, epoch, &mut state_table).await;

            // The minimum should be 6
            assert_eq!(
                managed_state.get_output(epoch, &state_table).await.unwrap(),
                Some(ScalarImpl::Int64(6))
            );
        }
    }

    #[tokio::test]
    async fn chaos_test_min() {
        chaos_test::<{ variants::EXTREME_MIN }>().await;
    }

    #[tokio::test]
    async fn chaos_test_max() {
        chaos_test::<{ variants::EXTREME_MAX }>().await;
    }

    async fn chaos_test<const EXTREME_TYPE: usize>() {
        let mut rng = thread_rng();
        let mut values_to_insert = (0..5000i64).collect_vec();
        values_to_insert.shuffle(&mut rng);
        let mut remaining_values = &values_to_insert[..];

        let store = MemoryStateStore::new();
        let order_type = if EXTREME_TYPE == variants::EXTREME_MAX {
            OrderType::Descending
        } else {
            OrderType::Ascending
        };
        let mut state_table =
            state_table_create_helper(store, TableId::from(0x2333), 1, order_type);

        let mut managed_state = GenericExtremeState::<_, I64Array, EXTREME_TYPE>::new(
            Some(3),
            0,
            PkDataTypes::new(),
            HashCode(567),
            None,
        )
        .unwrap();

        let mut heap = BTreeSet::new();

        loop {
            let insert_cnt = rng.gen_range(1..=10);
            let ops = vec![Op::Insert; insert_cnt];
            if remaining_values.len() < insert_cnt {
                break;
            }
            let batch = &remaining_values[..insert_cnt];
            let arr = I64Array::from_slice(&batch.iter().map(|x| Some(*x)).collect_vec()).unwrap();
            for data in batch {
                heap.insert(*data);
            }
            let epoch: u64 = 0;

            managed_state
                .apply_batch(&ops, None, &[&arr.into()], epoch, &mut state_table)
                .await
                .unwrap();

            remaining_values = &remaining_values[insert_cnt..];

            let delete_cnt = rng.gen_range(1..=10.min(heap.len()));
            let ops = vec![Op::Delete; delete_cnt];
            let mut delete_ids = (0..heap.len()).collect_vec();
            delete_ids.shuffle(&mut rng);
            let to_be_delete_idx: HashSet<usize> =
                delete_ids.iter().take(delete_cnt).cloned().collect();
            let mut to_be_delete = vec![];

            for (idx, item) in heap.iter().enumerate() {
                if to_be_delete_idx.contains(&idx) {
                    to_be_delete.push(*item);
                }
            }

            for item in &to_be_delete {
                heap.remove(item);
            }

            assert_eq!(to_be_delete.len(), delete_cnt);

            let arr =
                I64Array::from_slice(&to_be_delete.iter().map(|x| Some(*x)).collect_vec()).unwrap();
            managed_state
                .apply_batch(&ops, None, &[&arr.into()], epoch, &mut state_table)
                .await
                .unwrap();

            // flush to write batch and write to state store
            helper_flush(&mut managed_state, epoch, &mut state_table).await;

            let value = managed_state
                .get_output(epoch, &state_table)
                .await
                .unwrap()
                .map(|x| x.into_int64());

            match EXTREME_TYPE {
                variants::EXTREME_MAX => assert_eq!(value, heap.last().cloned()),
                variants::EXTREME_MIN => assert_eq!(value, heap.first().cloned()),
                _ => unimplemented!(),
            }
        }
    }

    async fn helper_flush<S: StateStore>(
        managed_state: &mut impl ManagedTableState<S>,
        epoch: u64,
        state_table: &mut RowBasedStateTable<S>,
    ) {
        managed_state.flush(state_table).unwrap();
        state_table.commit(epoch).await.unwrap();
    }

    #[tokio::test]
    async fn test_same_value_delete() {
        // In this test, we test this case:
        //
        // Delete 6, insert 6, and delete 6 in one epoch.
        // The 6 should be deleted from the state store.

        let store = MemoryStateStore::new();
        let mut managed_state = ManagedMinState::<_, I64Array>::new(
            Some(3),
            0,
            PkDataTypes::new(),
            HashCode(567),
            None,
        )
        .unwrap();
        let mut state_table =
            state_table_create_helper(store, TableId::from(0x2333), 1, OrderType::Ascending);

        assert!(!state_table.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(1), Some(2), Some(3), Some(4), Some(5), Some(7)])
                .unwrap()
                .into();
        let epoch: u64 = 0;

        managed_state
            .apply_batch(
                &[Op::Insert; 6],
                None,
                &[&value_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        managed_state
            .apply_batch(
                &[Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(6)]).unwrap().into()],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // Now we have 1 to 7 in the state store.
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        // Delete 6, insert 6, delete 6
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Insert, Op::Delete],
                None,
                &[&I64Array::from_slice(&[Some(6), Some(6), Some(6)])
                    .unwrap()
                    .into()],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        // 6 should be deleted by now
        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        let value_buffer = I64Array::from_slice(&[Some(1), Some(2), Some(3), Some(4), Some(5)])
            .unwrap()
            .into();

        // delete all remaining items
        managed_state
            .apply_batch(
                &[Op::Delete; 5],
                None,
                &[&value_buffer],
                epoch,
                &mut state_table,
            )
            .await
            .unwrap();

        helper_flush(&mut managed_state, epoch, &mut state_table).await;

        assert_eq!(
            managed_state.get_output(epoch, &state_table).await.unwrap(),
            Some(ScalarImpl::Int64(7))
        );
    }
}
