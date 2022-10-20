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

use futures::{pin_mut, StreamExt};
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Op, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{Datum, DatumRef, ScalarImpl};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;
use smallvec::SmallVec;

use super::state_cache::array_agg::ArrayAgg;
use super::state_cache::extreme::ExtremeAgg;
use super::state_cache::string_agg::StringAgg;
use super::state_cache::{CacheKey, GenericStateCache, StateCache};
use super::AggCall;
use crate::common::{iter_state_table, StateTableColumnMapping};
use crate::executor::{PkIndices, StreamExecutorResult};

/// Aggregation state as a materialization of input chunks.
///
/// For example, in `string_agg`, several useful columns are picked from input chunks and
/// stored in the state table when applying chunks, and the aggregation result is calculated
/// when need to get output.
pub struct MaterializedInputState<S: StateStore> {
    /// Group key to aggregate with group.
    /// None for simple agg, Some for group key of hash agg.
    group_key: Option<Row>,

    /// Argument column indices in input chunks.
    arg_col_indices: Vec<usize>,

    /// Argument column indices in state table.
    state_table_arg_col_indices: Vec<usize>,

    /// The columns to order by in input chunks.
    order_col_indices: Vec<usize>,

    /// The columns to order by in state table.
    state_table_order_col_indices: Vec<usize>,

    /// Cache of state table.
    cache: Box<dyn StateCache>,

    /// Serializer for cache key.
    cache_key_serializer: OrderedRowSerde,

    _phantom_data: PhantomData<S>,
}

impl<S: StateStore> MaterializedInputState<S> {
    /// Create an instance from [`AggCall`].
    pub fn new(
        agg_call: &AggCall,
        group_key: Option<&Row>,
        pk_indices: &PkIndices,
        col_mapping: &StateTableColumnMapping,
        row_count: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> Self {
        let arg_col_indices = agg_call.args.val_indices().to_vec();
        let (mut order_col_indices, mut order_types) =
            if matches!(agg_call.kind, AggKind::Min | AggKind::Max) {
                // `min`/`max` need not to order by any other columns, but have to
                // order by the agg value implicitly.
                let order_type = if agg_call.kind == AggKind::Min {
                    OrderType::Ascending
                } else {
                    OrderType::Descending
                };
                (vec![arg_col_indices[0]], vec![order_type])
            } else {
                agg_call
                    .order_pairs
                    .iter()
                    .map(|p| (p.column_idx, p.order_type))
                    .unzip()
            };

        let pk_len = pk_indices.len();
        order_col_indices.reserve_exact(pk_len);
        order_col_indices.extend(pk_indices.iter());
        order_types.reserve_exact(pk_len);
        order_types.extend(std::iter::repeat(OrderType::Ascending).take(pk_len));

        // map argument columns to state table column indices
        let state_table_arg_col_indices = arg_col_indices
            .iter()
            .map(|i| {
                col_mapping
                    .upstream_to_state_table(*i)
                    .expect("the argument columns must appear in the state table")
            })
            .collect_vec();

        // map order by columns to state table column indices
        let state_table_order_col_indices = order_col_indices
            .iter()
            .map(|i| {
                col_mapping
                    .upstream_to_state_table(*i)
                    .expect("the order columns must appear in the state table")
            })
            .collect_vec();

        let cache_key_data_types = order_col_indices
            .iter()
            .map(|i| input_schema[*i].data_type())
            .collect_vec();
        let cache_key_serializer = OrderedRowSerde::new(cache_key_data_types, order_types);

        let cache_capacity = if matches!(agg_call.kind, AggKind::Min | AggKind::Max) {
            extreme_cache_size
        } else {
            usize::MAX
        };

        let cache: Box<dyn StateCache> =
            match agg_call.kind {
                AggKind::Min | AggKind::Max | AggKind::FirstValue => Box::new(
                    GenericStateCache::new(ExtremeAgg, cache_capacity, row_count),
                ),
                AggKind::StringAgg => {
                    Box::new(GenericStateCache::new(StringAgg, cache_capacity, row_count))
                }
                AggKind::ArrayAgg => {
                    Box::new(GenericStateCache::new(ArrayAgg, cache_capacity, row_count))
                }
                _ => panic!(
                    "Agg kind `{}` is not expected to have materialized input state",
                    agg_call.kind
                ),
            };

        Self {
            group_key: group_key.cloned(),
            arg_col_indices,
            state_table_arg_col_indices,
            order_col_indices,
            state_table_order_col_indices,
            cache,
            cache_key_serializer,
            _phantom_data: PhantomData,
        }
    }

    /// Apply a chunk of data to the state cache.
    pub fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        self.cache.apply_batch(StateCacheInputBatch::new(
            ops,
            visibility,
            columns,
            &self.cache_key_serializer,
            &self.arg_col_indices,
            &self.order_col_indices,
        ));
        Ok(())
    }

    /// Get the output of the state.
    pub async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        if !self.cache.is_synced() {
            let all_data_iter = iter_state_table(state_table, self.group_key.as_ref()).await?;
            pin_mut!(all_data_iter);

            let mut cache_filler = self.cache.begin_syncing();
            #[for_await]
            for state_row in all_data_iter.take(cache_filler.capacity()) {
                let state_row = state_row?;
                let cache_key = {
                    let mut cache_key = Vec::new();
                    self.cache_key_serializer.serialize_datums(
                        self.state_table_order_col_indices
                            .iter()
                            .map(|col_idx| &(state_row.0)[*col_idx]),
                        &mut cache_key,
                    );
                    cache_key
                };
                let cache_value = self
                    .state_table_arg_col_indices
                    .iter()
                    .map(|i| state_row[*i].as_ref().map(ScalarImpl::as_scalar_ref_impl))
                    .collect();
                cache_filler.insert(cache_key, cache_value);
            }
        }
        debug_assert!(self.cache.is_synced());
        Ok(self.cache.get_output())
    }
}

pub struct StateCacheInputBatch<'a> {
    idx: usize,
    ops: Ops<'a>,
    visibility: Option<&'a Bitmap>,
    columns: &'a [&'a ArrayImpl],
    cache_key_serializer: &'a OrderedRowSerde,
    arg_col_indices: &'a [usize],
    order_col_indices: &'a [usize],
}

impl<'a> StateCacheInputBatch<'a> {
    fn new(
        ops: Ops<'a>,
        visibility: Option<&'a Bitmap>,
        columns: &'a [&'a ArrayImpl],
        cache_key_serializer: &'a OrderedRowSerde,
        arg_col_indices: &'a [usize],
        order_col_indices: &'a [usize],
    ) -> Self {
        let first_idx = visibility.map_or(0, |v| v.next_set_bit(0).unwrap_or(ops.len()));
        Self {
            idx: first_idx,
            ops,
            visibility,
            columns,
            cache_key_serializer,
            arg_col_indices,
            order_col_indices,
        }
    }
}

impl<'a> Iterator for StateCacheInputBatch<'a> {
    type Item = (Op, CacheKey, SmallVec<[DatumRef<'a>; 2]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.ops.len() {
            None
        } else {
            let op = self.ops[self.idx];
            let key = {
                let mut key = Vec::new();
                self.cache_key_serializer.serialize_datum_refs(
                    self.order_col_indices
                        .iter()
                        .map(|col_idx| self.columns[*col_idx].value_at(self.idx)),
                    &mut key,
                );
                key
            };
            let value = self
                .arg_col_indices
                .iter()
                .map(|col_idx| self.columns[*col_idx].value_at(self.idx))
                .collect();
            self.idx = self.visibility.map_or(self.idx + 1, |v| {
                v.next_set_bit(self.idx + 1).unwrap_or(self.ops.len())
            });
            Some((op, key, value))
        }
    }
}
