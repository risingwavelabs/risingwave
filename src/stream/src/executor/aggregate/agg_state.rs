// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_pb::stream_plan::PbAggNodeVersion;
use risingwave_storage::StateStore;

use super::minput::MaterializedInputState;
use crate::common::StateTableColumnMapping;
use crate::common::table::state_table::StateTable;
use crate::executor::aggregate::agg_group::{AggStateCacheStats, GroupKey};
use crate::executor::{PkIndices, StreamExecutorResult};

/// Represents the persistent storage of aggregation state.
pub enum AggStateStorage<S: StateStore> {
    /// The state is stored as a value in the intermediate state table.
    Value,

    /// The state is stored as a materialization of input chunks, in a standalone state table.
    /// `mapping` describes the mapping between the columns in the state table and the input
    /// chunks. `order_columns` list the index and order type of sort keys.
    MaterializedInput {
        table: StateTable<S>,
        mapping: StateTableColumnMapping,
        order_columns: Vec<ColumnOrder>,
    },
}

/// State for single aggregation call. It manages the state cache and interact with the
/// underlying state store if necessary.
pub enum AggState {
    /// State as a single scalar value.
    /// e.g. `count`, `sum`, append-only `min`/`max`.
    Value(AggregateState),

    /// State as materialized input chunk, e.g. non-append-only `min`/`max`, `string_agg`.
    MaterializedInput(Box<MaterializedInputState>),
}

impl EstimateSize for AggState {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Self::Value(state) => state.estimated_heap_size(),
            Self::MaterializedInput(state) => state.estimated_size(),
        }
    }
}

impl AggState {
    /// Create an [`AggState`] from a given [`AggCall`].
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        version: PbAggNodeVersion,
        agg_call: &AggCall,
        agg_func: &BoxedAggregateFunction,
        storage: &AggStateStorage<impl StateStore>,
        encoded_state: Option<&Datum>,
        pk_indices: &PkIndices,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        Ok(match storage {
            AggStateStorage::Value => {
                let state = match encoded_state {
                    Some(encoded) => agg_func.decode_state(encoded.clone())?,
                    None => agg_func.create_state()?,
                };
                Self::Value(state)
            }
            AggStateStorage::MaterializedInput {
                mapping,
                order_columns,
                ..
            } => Self::MaterializedInput(Box::new(MaterializedInputState::new(
                version,
                agg_call,
                pk_indices,
                order_columns,
                mapping,
                extreme_cache_size,
                input_schema,
            )?)),
        })
    }

    /// Apply input chunk to the state.
    pub async fn apply_chunk(
        &mut self,
        chunk: &StreamChunk,
        call: &AggCall,
        func: &BoxedAggregateFunction,
        visibility: Bitmap,
    ) -> StreamExecutorResult<()> {
        match self {
            Self::Value(state) => {
                let chunk = chunk.project_with_vis(call.args.val_indices(), visibility);
                func.update(state, &chunk).await?;
                Ok(())
            }
            Self::MaterializedInput(state) => {
                // the input chunk for minput is unprojected
                let chunk = chunk.clone_with_vis(visibility);
                state.apply_chunk(&chunk)
            }
        }
    }

    /// Get the output of aggregations.
    pub async fn get_output(
        &mut self,
        storage: &AggStateStorage<impl StateStore>,
        func: &BoxedAggregateFunction,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<(Datum, AggStateCacheStats)> {
        match self {
            Self::Value(state) => {
                debug_assert!(matches!(storage, AggStateStorage::Value));
                Ok((func.get_result(state).await?, AggStateCacheStats::default()))
            }
            Self::MaterializedInput(state) => {
                let state_table = must_match!(
                    storage,
                    AggStateStorage::MaterializedInput { table, .. } => table
                );
                state.get_output(state_table, group_key, func).await
            }
        }
    }

    /// Reset the value state to initial state.
    pub fn reset(&mut self, func: &BoxedAggregateFunction) -> StreamExecutorResult<()> {
        if let Self::Value(state) = self {
            // now only value states need to be reset
            *state = func.create_state()?;
        }
        Ok(())
    }
}
