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

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Op, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::table_state::{
    GenericExtremeState, ManagedArrayAggState, ManagedStringAggState, ManagedTableState,
};
use super::AggCall;
use crate::common::StateTableColumnMapping;
use crate::executor::{PkIndices, StreamExecutorResult};

/// Aggregation state as a materialization of input chunks.
///
/// For example, in `string_agg`, several useful columns are picked from input chunks and
/// stored in the state table when applying chunks, and the aggregation result is calculated
/// when need to get output.
pub struct MaterializedInputState<S: StateStore> {
    kind: AggKind,
    arg_col_indices: Vec<usize>,
    col_mapping: StateTableColumnMapping,
    inner: Box<dyn ManagedTableState<S>>,
}

impl<S: StateStore> MaterializedInputState<S> {
    /// Create an instance from [`AggCall`].
    pub fn new(
        agg_call: &AggCall,
        group_key: Option<&Row>,
        pk_indices: &PkIndices,
        col_mapping: StateTableColumnMapping,
        row_count: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> Self {
        let arg_col_indices = agg_call.args.val_indices();
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

        Self {
            kind: agg_call.kind,
            arg_col_indices: arg_col_indices.to_vec(),
            col_mapping,
            inner: match agg_call.kind {
                AggKind::Min | AggKind::Max | AggKind::FirstValue => {
                    Box::new(GenericExtremeState::new(
                        state_table_arg_col_indices,
                        state_table_order_col_indices,
                        group_key,
                        row_count,
                        extreme_cache_size,
                        cache_key_serializer,
                    ))
                }
                AggKind::StringAgg => Box::new(ManagedStringAggState::new(
                    state_table_arg_col_indices,
                    state_table_order_col_indices,
                    group_key,
                    row_count,
                    cache_key_serializer,
                )),
                AggKind::ArrayAgg => Box::new(ManagedArrayAggState::new(
                    state_table_arg_col_indices,
                    state_table_order_col_indices,
                    group_key,
                    row_count,
                    cache_key_serializer,
                )),
                _ => panic!(
                    "Agg kind `{}` is not expected to have materialized input state",
                    agg_call.kind
                ),
            },
        }
    }

    /// Apply a chunk of data to the state.
    pub async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        let should_skip_null_value =
            matches!(self.kind, AggKind::Min | AggKind::Max | AggKind::StringAgg);

        for (i, op) in ops
            .iter()
            .enumerate()
            // skip invisible
            .filter(|(i, _)| visibility.map(|x| x.is_set(*i)).unwrap_or(true))
            // skip null input if should
            .filter(|(i, _)| {
                if should_skip_null_value {
                    columns[self.arg_col_indices[0]].null_bitmap().is_set(*i)
                } else {
                    true
                }
            })
        {
            let state_row = Row::new(
                self.col_mapping
                    .upstream_columns()
                    .iter()
                    .map(|col_idx| columns[*col_idx].datum_at(i))
                    .collect(),
            );
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.inner.insert(&state_row);
                    state_table.insert(state_row);
                }
                Op::Delete | Op::UpdateDelete => {
                    self.inner.delete(&state_row);
                    state_table.delete(state_row);
                }
            }
        }

        Ok(())
    }

    /// Get the output of the state.
    pub async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        self.inner.get_output(state_table).await
    }
}
