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

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::Datum;
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
        let (mut order_col_indices, mut order_types): (Vec<_>, Vec<_>) = agg_call
            .order_pairs
            .iter()
            .map(|p| (p.column_idx, p.order_type))
            .unzip();
        if matches!(agg_call.kind, AggKind::Min | AggKind::Max) {
            // `min`/`max` need not to order by any other columns
            order_col_indices.clear();
            order_types.clear();
            order_col_indices.push(arg_col_indices[0]);
            if agg_call.kind == AggKind::Min {
                order_types.push(OrderType::Ascending);
            } else {
                order_types.push(OrderType::Descending);
            }
        }
        Self {
            inner: match agg_call.kind {
                AggKind::Min | AggKind::Max | AggKind::FirstValue => {
                    Box::new(GenericExtremeState::new(
                        arg_col_indices,
                        &order_col_indices,
                        &order_types,
                        group_key,
                        pk_indices,
                        col_mapping,
                        row_count,
                        extreme_cache_size,
                        input_schema,
                    ))
                }
                AggKind::StringAgg => Box::new(ManagedStringAggState::new(
                    arg_col_indices,
                    &order_col_indices,
                    &order_types,
                    group_key,
                    pk_indices,
                    col_mapping,
                    row_count,
                )),
                AggKind::ArrayAgg => Box::new(ManagedArrayAggState::new(
                    arg_col_indices,
                    &order_col_indices,
                    &order_types,
                    group_key,
                    pk_indices,
                    col_mapping,
                    row_count,
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
        self.inner
            .apply_chunk(ops, visibility, columns, state_table)
            .await
    }

    /// Get the output of the state.
    pub async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        self.inner.get_output(state_table).await
    }
}
