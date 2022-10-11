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
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::table_state::{
    GenericExtremeState, ManagedArrayAggState, ManagedStringAggState, ManagedTableState,
};
use super::AggCall;
use crate::common::StateTableColumnMapping;
use crate::executor::{PkIndices, StreamExecutorResult};

pub struct MaterializedInputState<S: StateStore> {
    inner: Box<dyn ManagedTableState<S>>,
}

impl<S: StateStore> MaterializedInputState<S> {
    pub fn new(
        agg_call: &AggCall,
        group_key: Option<&Row>,
        pk_indices: &PkIndices,
        col_mapping: &StateTableColumnMapping,
        row_count: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> Self {
        match agg_call.kind {
            AggKind::Max | AggKind::Min => MaterializedInputState {
                inner: Box::new(GenericExtremeState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    col_mapping.clone(),
                    row_count,
                    extreme_cache_size,
                    input_schema,
                )),
            },
            AggKind::StringAgg => MaterializedInputState {
                inner: Box::new(ManagedStringAggState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    col_mapping.clone(),
                    row_count,
                )),
            },
            AggKind::ArrayAgg => MaterializedInputState {
                inner: Box::new(ManagedArrayAggState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    col_mapping.clone(),
                    row_count,
                )),
            },
            _ => panic!("Agg call `{}` shouldn't reach here", agg_call.kind),
        }
    }

    pub async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, columns));
        self.inner
            .apply_chunk(ops, visibility, columns, state_table)
            .await
    }

    pub async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum> {
        self.inner.get_output(state_table).await
    }
}
