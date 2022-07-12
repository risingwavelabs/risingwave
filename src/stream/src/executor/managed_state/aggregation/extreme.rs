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

use std::marker::PhantomData;

use async_trait::async_trait;
use futures::{pin_mut, StreamExt};
use itertools::Itertools;
use madsim::collections::BTreeMap;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::{Array, ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::HashCode;
use risingwave_common::types::*;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::StateStore;
use smallvec::SmallVec;

use crate::executor::aggregation::{AggArgs, AggCall};
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkDataTypes;

pub type ManagedMinState<S, A> = GenericExtremeState<S, A, { variants::EXTREME_MIN }>;
pub type ManagedMaxState<S, A> = GenericExtremeState<S, A, { variants::EXTREME_MAX }>;

type ExtremePkItem = Datum;

pub type ExtremePk = SmallVec<[ExtremePkItem; 1]>;

/// All possible extreme types.
pub mod variants {
    pub const EXTREME_MIN: usize = 0;
    pub const EXTREME_MAX: usize = 1;
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
    /// Top N elements in the state, which stores the mapping of (sort key, pk) ->
    /// (original sort key). This BTreeMap always maintain the elements that we are sure
    /// it's top n.
    top_n: BTreeMap<(Option<A::OwnedItem>, ExtremePk), Datum>,

    /// Number of items in the state including those not in top n cache but in state store.
    total_count: usize,

    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,

    // TODO: Remove this phantom to get rid of S: StateStore.
    _phantom_data: PhantomData<S>,

    /// The upstream pk. Assembled as pk of relational table.
    upstream_pk_len: usize,

    /// Primary key to look up in relational table. For value state, there is only one row.
    /// If None, the pk is empty vector (simple agg). If not None, the pk is group key (hash agg).
    group_key: Option<Row>,
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
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Datum>;

    /// Check if this state needs a flush.
    fn is_dirty(&self) -> bool;

    /// Flush the internal state to a write batch.
    fn flush(&mut self, state_table: &mut StateTable<S>) -> StreamExecutorResult<()>;
}

impl<S: StateStore, A: Array, const EXTREME_TYPE: usize> GenericExtremeState<S, A, EXTREME_TYPE>
where
    A::OwnedItem: Ord,
    for<'a> &'a A: From<&'a ArrayImpl>,
{
    /// Create a managed min state. When `top_n_count` is `None`, the cache will
    /// always be retained when flushing the managed state. Otherwise, we will only retain n entries
    /// after each flush.
    pub async fn new(
        top_n_count: Option<usize>,
        row_count: usize,
        pk_data_types: PkDataTypes,
        _group_key_hash_code: HashCode,
        group_key: Option<&Row>,
    ) -> StreamExecutorResult<Self> {
        // Create the internal state based on the value we get.
        Ok(Self {
            top_n: BTreeMap::new(),
            total_count: row_count,
            _phantom_data: PhantomData::default(),
            upstream_pk_len: pk_data_types.len(),
            top_n_count,
            group_key: group_key.cloned(),
        })
    }

    fn pk_length(&self) -> usize {
        self.upstream_pk_len
    }

    /// Retain only top n elements in the cache
    fn retain_top_n(&mut self) {
        if let Some(count) = self.top_n_count {
            match EXTREME_TYPE {
                variants::EXTREME_MIN => {
                    while self.top_n.len() > count {
                        self.top_n.pop_last();
                    }
                }
                variants::EXTREME_MAX => {
                    while self.top_n.len() > count {
                        self.top_n.pop_first();
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    /// Apply a batch of data to the state.
    async fn apply_batch_inner(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));

        // For extreme state, there's only one column for data, and following columns are for
        // primary keys!
        assert_eq!(
            data.len(),
            1 + self.pk_length(),
            "mismatched data input with pk_length"
        );

        let data_column: &A = data[0].into();
        let pk_columns = (0..self.pk_length()).map(|idx| data[idx + 1]).collect_vec();

        // `self.top_n` only contains parts of the top keys. When applying batch, we only:
        // 1. Insert keys that is in the range of top n, so that we only maintain the top keys we
        // are sure the minimum.
        // 2. Delete keys if exists in top_n cache.
        // If the top n cache becomes empty after applying the batch, we will merge data from
        // flush_buffer and state store when getting the output.

        for (id, (op, key)) in ops.iter().zip_eq(data_column.iter()).enumerate() {
            let visible = visibility.map(|x| x.is_set(id).unwrap()).unwrap_or(true);
            if !visible {
                continue;
            }

            // sort key may be null
            let key: Option<A::OwnedItem> = option_to_owned_scalar(&key);

            // Concat pk with the original key to create a composed key
            let composed_key = (
                key.clone(),
                // Collect pk from columns
                pk_columns
                    .iter()
                    .map(|col| col.datum_at(id))
                    .collect::<ExtremePk>(),
            );

            // Get relational pk and value.
            let sort_key = key.map(|key| key.into());
            let relational_value =
                self.get_relational_value(sort_key.clone(), composed_key.1.clone());

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let mut do_insert = false;
                    if self.total_count == self.top_n.len() {
                        // Data fully cached in memory
                        do_insert = true;
                    } else {
                        match EXTREME_TYPE {
                            variants::EXTREME_MIN => {
                                if let Some((last_key, _)) = self.top_n.last_key_value() {
                                    if &composed_key < last_key {
                                        do_insert = true;
                                    }
                                }
                            }
                            variants::EXTREME_MAX => {
                                if let Some((first_key, _)) = self.top_n.first_key_value() {
                                    if &composed_key > first_key {
                                        do_insert = true;
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    if do_insert {
                        self.top_n.insert(composed_key.clone(), sort_key);
                    }
                    state_table.insert(relational_value)?;
                    self.total_count += 1;
                }
                Op::Delete | Op::UpdateDelete => {
                    self.top_n.remove(&composed_key);
                    state_table.delete(relational_value)?;
                    self.total_count -= 1;
                }
            }
        }

        self.retain_top_n();

        Ok(())
    }

    fn get_output_from_cache(&self) -> Datum {
        match EXTREME_TYPE {
            variants::EXTREME_MIN => {
                if let Some((_, v)) = self.top_n.first_key_value() {
                    return v.clone();
                }
            }
            variants::EXTREME_MAX => {
                if let Some((_, v)) = self.top_n.last_key_value() {
                    return v.clone();
                }
            }
            _ => unimplemented!(),
        }
        None
    }

    async fn get_output_inner(
        &mut self,
        epoch: u64,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        // To make things easier, we do not allow get_output before flushing. Otherwise we will need
        // to merge data from flush_buffer and state store, which is hard to implement.
        //
        // To make ExtremeState produce the correct result, the write batch must be flushed into the
        // state store before getting the output. Note that calling `.flush()` is not enough, as it
        // only generates a write batch without flushing to store.

        // Firstly, check if datum is available in cache.
        if let Some(v) = self.get_output_from_cache() {
            Ok(Some(v))
        } else {
            // Then, fetch from the state store.
            //
            // To future developers: please make **SURE** you have taken `EXTREME_TYPE` into
            // account. EXTREME_MIN and EXTREME_MAX will significantly impact the
            // following logic.
            let all_data_iter = if let Some(group_key) = self.group_key.as_ref() {
                state_table.iter_with_pk_prefix(group_key, epoch).await?
            } else {
                state_table.iter(epoch).await?
            };
            pin_mut!(all_data_iter);

            for _ in 0..self.top_n_count.unwrap_or(usize::MAX) {
                if let Some(inner) = all_data_iter.next().await {
                    let row = inner?;

                    let group_key_len = self.group_key.as_ref().map_or(0, |row| row.size());
                    // Get the agg call value. Same as sort key.
                    let value = row[group_key_len].clone();

                    // Get sort key and extreme pk.
                    let sort_key = value.as_ref().map(|v| v.clone().try_into().unwrap());
                    let mut extreme_pk = ExtremePk::with_capacity(1);
                    // The layout is group_key/sort_key/extreme_pk. So the range
                    // should be [group_key_len + 1, row.0.len()).
                    for extreme_pk_index in group_key_len + 1..row.0.len() {
                        extreme_pk.push(row[extreme_pk_index].clone());
                    }

                    self.top_n.insert((sort_key, extreme_pk), value);
                } else {
                    break;
                }
            }

            if let Some(v) = self.get_output_from_cache() {
                Ok(Some(v))
            } else {
                Ok(None)
            }
        }
    }

    /// TODO: Revisit all `.flush` and remove if necessary. Because now the state table flush is
    /// controlled by executor, the `.flush_inner` of agg states do not persist.
    fn flush_inner(&mut self) -> StreamExecutorResult<()> {
        self.retain_top_n();
        Ok(())
    }

    /// Assemble value for relational table used by extreme state. Should be `group_key` +
    /// `sort_key` + input pk + agg call value.
    fn get_relational_value(&self, sort_key: Datum, extreme_pk: ExtremePk) -> Row {
        let mut sort_key_vec = if let Some(group_key) = self.group_key.as_ref() {
            group_key.0.to_vec()
        } else {
            vec![]
        };
        sort_key_vec.push(sort_key);
        sort_key_vec.extend(extreme_pk.into_iter());
        Row::new(sort_key_vec)
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
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        self.apply_batch_inner(ops, visibility, data, state_table)
            .await
    }

    async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        self.get_output_inner(epoch, state_table).await
    }

    /// Check if this state needs a flush.
    /// TODO: Remove this.
    fn is_dirty(&self) -> bool {
        unreachable!("Should not call this function anymore, check state table for dirty data");
    }

    fn flush(&mut self, _state_table: &mut StateTable<S>) -> StreamExecutorResult<()> {
        self.flush_inner()
    }
}

pub async fn create_streaming_extreme_state<S: StateStore>(
    agg_call: AggCall,
    row_count: usize,
    top_n_count: Option<usize>,
    pk_data_types: PkDataTypes,
    key_hash_code: Option<HashCode>,
    pk: Option<&Row>,
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

            match (agg_call.kind, agg_call.return_type.clone()) {
                $(
                    (AggKind::Max, $( $kind )|+) => Ok(Box::new(
                        ManagedMaxState::<_, $array>::new(
                            top_n_count,
                            row_count,
                            pk_data_types,
                            key_hash_code.unwrap_or_default(),
                            pk
                        ).await?,
                    )),
                    (AggKind::Min, $( $kind )|+) => Ok(Box::new(
                        ManagedMinState::<_, $array>::new(
                            top_n_count,
                            row_count,
                            pk_data_types,
                            key_hash_code.unwrap_or_default(),
                            pk
                        ).await?,
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
    use itertools::Itertools;
    use madsim::collections::{BTreeSet, HashSet};
    use madsim::rand::prelude::*;
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
        .await
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
        .await
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
    ) -> StateTable<S> {
        let mut column_descs = Vec::with_capacity(column_cnt);
        for id in 0..column_cnt {
            column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(id as i32),
                DataType::Int64,
            ));
        }
        let relational_pk_len = column_descs.len();
        StateTable::new_without_distribution(
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
        .await
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
        .await
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
        .await
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
        .await
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
        state_table: &mut StateTable<S>,
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
        .await
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
