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
use risingwave_expr::expr::AggKind;
use risingwave_storage::StateStore;

use super::in_memory_value::ManagedValueState;
use super::table_state::{
    GenericExtremeState, ManagedArrayAggState, ManagedStringAggState, ManagedTableState,
};
use super::{AggCall, AggStateTable};
use crate::executor::{PkIndices, StreamExecutorResult};

/// Verify if the data going through the state is valid by checking if `ops.len() ==
/// visibility.len() == columns[x].len()`.
fn verify_chunk(ops: Ops<'_>, visibility: Option<&Bitmap>, columns: &[&ArrayImpl]) -> bool {
    let mut all_lengths = vec![ops.len()];
    if let Some(visibility) = visibility {
        all_lengths.push(visibility.len());
    }
    all_lengths.extend(columns.iter().map(|x| x.len()));
    all_lengths.iter().min() == all_lengths.iter().max()
}

/// State for single aggregation call. It manages the cache and interact with the
/// underlying state store.
pub enum AggState<S: StateStore> {
    /// State as in-memory scalar value, e.g. `count`, `sum`, append-only `min`/`max`.
    InMemoryValue(ManagedValueState),

    /// State as materialized input chunk, e.g. non-append-only `min`/`max`, `string_agg`.
    MaterializedInput(Box<dyn ManagedTableState<S>>),
}

impl<S: StateStore> AggState<S> {
    /// Create an [`AggState`] from a given [`AggCall`].
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        agg_call: &AggCall,
        agg_state_table: Option<&AggStateTable<S>>,
        row_count: usize,
        prev_output: Option<&Datum>,
        pk_indices: &PkIndices,
        group_key: Option<&Row>,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        match agg_call.kind {
            AggKind::Avg | AggKind::Count | AggKind::Sum | AggKind::ApproxCountDistinct => Ok(
                Self::InMemoryValue(ManagedValueState::new(agg_call, prev_output.cloned())?),
            ),
            // optimization: use single-value state for append-only min/max
            AggKind::Max | AggKind::Min | AggKind::FirstValue if agg_call.append_only => Ok(
                Self::InMemoryValue(ManagedValueState::new(agg_call, prev_output.cloned())?),
            ),
            AggKind::Max | AggKind::Min | AggKind::FirstValue => {
                Ok(Self::MaterializedInput(Box::new(GenericExtremeState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    agg_state_table
                        .expect("non-append-only min/max must have state table")
                        .mapping
                        .clone(),
                    row_count,
                    extreme_cache_size,
                    input_schema,
                ))))
            }
            AggKind::StringAgg => Ok(Self::MaterializedInput(Box::new(
                ManagedStringAggState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    agg_state_table
                        .expect("string_agg must have state table")
                        .mapping
                        .clone(),
                    row_count,
                ),
            ))),
            AggKind::ArrayAgg => Ok(Self::MaterializedInput(Box::new(
                ManagedArrayAggState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    agg_state_table
                        .expect("array_agg must have state table")
                        .mapping
                        .clone(),
                    row_count,
                ),
            ))),
        }
    }

    /// Apply input chunk to the state.
    pub async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        agg_state_table: Option<&mut AggStateTable<S>>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(verify_chunk(ops, visibility, columns));
        match self {
            Self::InMemoryValue(state) => state.apply_chunk(ops, visibility, columns),
            Self::MaterializedInput(state) => {
                state
                    .apply_chunk(
                        ops,
                        visibility,
                        columns,
                        &mut agg_state_table
                            .expect("managed table state must have state table")
                            .table,
                    )
                    .await
            }
        }
    }

    /// Get the output of the state.
    pub async fn get_output(
        &mut self,
        agg_state_table: Option<&AggStateTable<S>>,
    ) -> StreamExecutorResult<Datum> {
        match self {
            Self::InMemoryValue(state) => Ok(state.get_output()),
            Self::MaterializedInput(state) => {
                state
                    .get_output(
                        &agg_state_table
                            .expect("managed table state must have state table")
                            .table,
                    )
                    .await
            }
        }
    }
}
