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

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_common_proc_macro::EstimateSize;
use risingwave_expr::agg::AggCall;
use risingwave_storage::StateStore;

use super::minput::MaterializedInputState;
use super::table::TableState;
use super::value::ValueState;
use super::GroupKey;
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;
use crate::executor::{PkIndices, StreamExecutorResult};

/// Represents the persistent storage of aggregation state.
pub enum AggStateStorage<S: StateStore> {
    /// The state is stored in the result table. No standalone state table is needed.
    ResultValue,

    /// The state is stored in a single state table whose schema is deduced by frontend and backend
    /// with implicit consensus.
    Table { table: StateTable<S> },

    /// The state is stored as a materialization of input chunks, in a standalone state table.
    /// `mapping` describes the mapping between the columns in the state table and the input
    /// chunks.
    MaterializedInput {
        table: StateTable<S>,
        mapping: StateTableColumnMapping,
    },
}

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

/// State for single aggregation call. It manages the state cache and interact with the
/// underlying state store if necessary.
#[derive(EstimateSize)]
pub enum AggState<S: StateStore> {
    /// State as single scalar value, e.g. `count`, `sum`, append-only `min`/`max`.
    Value(ValueState),

    /// State as a single state table whose schema is deduced by frontend and backend with implicit
    /// consensus, e.g. append-only `single_phase_approx_count_distinct`.
    Table(TableState<S>),

    /// State as materialized input chunk, e.g. non-append-only `min`/`max`, `string_agg`.
    MaterializedInput(MaterializedInputState<S>),
}

impl<S: StateStore> AggState<S> {
    /// Create an [`AggState`] from a given [`AggCall`].
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        agg_call: &AggCall,
        storage: &AggStateStorage<S>,
        prev_output: Option<&Datum>,
        pk_indices: &PkIndices,
        group_key: Option<&GroupKey>,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        Ok(match storage {
            AggStateStorage::ResultValue => {
                Self::Value(ValueState::new(agg_call, prev_output.cloned())?)
            }
            AggStateStorage::Table { table } => {
                Self::Table(TableState::new(agg_call, table, group_key).await?)
            }
            AggStateStorage::MaterializedInput { mapping, .. } => {
                Self::MaterializedInput(MaterializedInputState::new(
                    agg_call,
                    pk_indices,
                    mapping,
                    extreme_cache_size,
                    input_schema,
                ))
            }
        })
    }

    /// Apply input chunk to the state.
    pub fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        storage: &mut AggStateStorage<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(verify_chunk(ops, visibility, columns));
        match self {
            Self::Value(state) => {
                debug_assert!(matches!(storage, AggStateStorage::ResultValue));
                state.apply_chunk(ops, visibility, columns)
            }
            Self::Table(state) => {
                debug_assert!(matches!(storage, AggStateStorage::Table { .. }));
                state.apply_chunk(ops, visibility, columns)
            }
            Self::MaterializedInput(state) => {
                debug_assert!(matches!(storage, AggStateStorage::MaterializedInput { .. }));
                state.apply_chunk(ops, visibility, columns)
            }
        }
    }

    /// Get the output of the state.
    pub async fn get_output(
        &mut self,
        storage: &AggStateStorage<S>,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<Datum> {
        match self {
            Self::Value(state) => {
                debug_assert!(matches!(storage, AggStateStorage::ResultValue));
                Ok(state.get_output())
            }
            Self::Table(state) => {
                debug_assert!(matches!(storage, AggStateStorage::Table { .. }));
                state.get_output()
            }
            Self::MaterializedInput(state) => {
                let state_table = must_match!(
                    storage,
                    AggStateStorage::MaterializedInput { table, .. } => table
                );
                state.get_output(state_table, group_key).await
            }
        }
    }

    /// Reset the value state to initial state.
    pub fn reset(&mut self) {
        if let Self::Value(state) = self {
            // now only value states need to be reset
            state.reset();
        }
    }
}
