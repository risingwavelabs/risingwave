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

use risingwave_common::array::{StreamChunk, Vis};
use risingwave_common::catalog::Schema;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_expr::agg::{build, AggCall, BoxedAggState};
use risingwave_storage::StateStore;

use super::minput::MaterializedInputState;
use super::table::TableState;
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

/// State for single aggregation call. It manages the state cache and interact with the
/// underlying state store if necessary.
#[derive(EstimateSize)]
pub enum AggState {
    /// State as single scalar value and is same as output.
    /// e.g. `count`, `sum`, append-only `min`/`max`.
    Value(BoxedAggState),

    /// State as single scalar value but is different from output.
    /// e.g. append-only `single_phase_approx_count_distinct`.
    Table(TableState),

    /// State as materialized input chunk, e.g. non-append-only `min`/`max`, `string_agg`.
    MaterializedInput(MaterializedInputState),
}

impl AggState {
    /// Create an [`AggState`] from a given [`AggCall`].
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        agg_call: &AggCall,
        storage: &AggStateStorage<impl StateStore>,
        prev_output: Option<&Datum>,
        pk_indices: &PkIndices,
        group_key: Option<&GroupKey>,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        Ok(match storage {
            AggStateStorage::ResultValue => {
                let mut state = build(agg_call)?;
                if let Some(prev) = prev_output {
                    state.set_state(prev.clone());
                }
                Self::Value(state)
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
                )?)
            }
        })
    }

    /// Apply input chunk to the state.
    pub async fn apply_chunk(
        &mut self,
        chunk: &StreamChunk,
        call: &AggCall,
        visibility: Vis,
    ) -> StreamExecutorResult<()> {
        match self {
            Self::Value(state) => {
                let chunk = chunk.project_with_vis(call.args.val_indices(), visibility);
                state.update(&chunk).await?;
                Ok(())
            }
            Self::Table(state) => {
                let chunk = chunk.project_with_vis(call.args.val_indices(), visibility);
                state.apply_chunk(&chunk).await
            }
            Self::MaterializedInput(state) => {
                // the input chunk for minput is unprojected
                let mut chunk = chunk.clone();
                chunk.set_vis(visibility);
                state.apply_chunk(&chunk)
            }
        }
    }

    /// Get the output of the state.
    pub async fn get_output(
        &mut self,
        storage: &AggStateStorage<impl StateStore>,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<Datum> {
        match self {
            Self::Value(state) => {
                debug_assert!(matches!(storage, AggStateStorage::ResultValue));
                Ok(state.get_state())
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
