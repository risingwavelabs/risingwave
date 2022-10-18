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
use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::minput::MaterializedInputState;
use super::value::ValueState;
use super::AggCall;
use crate::common::StateTableColumnMapping;
use crate::executor::{PkIndices, StreamExecutorResult};

/// Represents the persistent storage of aggregation state.
pub enum AggStateStorage<S: StateStore> {
    /// The state is stored in the result table. No standalone state table is needed.
    ResultValue,

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
pub enum AggState<S: StateStore> {
    /// State as single scalar value, e.g. `count`, `sum`, append-only `min`/`max`.
    Value(ValueState),

    /// State as materialized input chunk, e.g. non-append-only `min`/`max`, `string_agg`.
    MaterializedInput(MaterializedInputState<S>),
}

impl<S: StateStore> AggState<S> {
    /// Create an [`AggState`] from a given [`AggCall`].
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        agg_call: &AggCall,
        storage: &AggStateStorage<S>,
        row_count: usize,
        prev_output: Option<&Datum>,
        pk_indices: &PkIndices,
        group_key: Option<&Row>,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        Ok(match storage {
            AggStateStorage::ResultValue => {
                Self::Value(ValueState::new(agg_call, prev_output.cloned())?)
            }
            AggStateStorage::MaterializedInput { mapping, .. } => {
                Self::MaterializedInput(MaterializedInputState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    mapping.clone(),
                    row_count,
                    extreme_cache_size,
                    input_schema,
                ))
            }
        })
    }

    /// Apply input chunk to the state.
    pub async fn apply_chunk(
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
            Self::MaterializedInput(state) => {
                let state_table =
                    must_match!(storage, AggStateStorage::MaterializedInput { table, .. } => table);
                state
                    .apply_chunk(ops, visibility, columns, state_table)
                    .await
            }
        }
    }

    /// Get the output of the state.
    pub async fn get_output(
        &mut self,
        storage: &AggStateStorage<S>,
    ) -> StreamExecutorResult<Datum> {
        match self {
            Self::Value(state) => {
                debug_assert!(matches!(storage, AggStateStorage::ResultValue));
                Ok(state.get_output())
            }
            Self::MaterializedInput(state) => {
                let state_table = must_match!(
                    storage,
                    AggStateStorage::MaterializedInput { table, .. } => table
                );
                state.get_output(state_table).await
            }
        }
    }
}
