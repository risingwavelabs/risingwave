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

pub use agg_call::*;
pub use agg_group::*;
use anyhow::anyhow;
use risingwave_common::array::column::Column;
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::{DataChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::ActorContextRef;
use crate::common::{InfallibleExpression, StateTableColumnMapping};
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::Executor;

mod agg_call;
mod agg_group;
pub mod agg_impl;

/// Generate [`crate::executor::HashAggExecutor`]'s schema from `input`, `agg_calls` and
/// `group_key_indices`. For [`crate::executor::HashAggExecutor`], the group key indices should
/// be provided.
pub fn generate_agg_schema(
    input: &dyn Executor,
    agg_calls: &[AggCall],
    group_key_indices: Option<&[usize]>,
) -> Schema {
    let aggs = agg_calls
        .iter()
        .map(|agg| Field::unnamed(agg.return_type.clone()));

    let fields = if let Some(key_indices) = group_key_indices {
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].clone());

        keys.chain(aggs).collect()
    } else {
        aggs.collect()
    };

    Schema { fields }
}

pub fn agg_call_filter_res(
    ctx: &ActorContextRef,
    identity: &str,
    agg_call: &AggCall,
    columns: &Vec<Column>,
    visibility: Option<&Bitmap>,
    capacity: usize,
) -> StreamExecutorResult<Option<Bitmap>> {
    if let Some(ref filter) = agg_call.filter {
        let vis = Vis::from(
            visibility
                .cloned()
                .unwrap_or_else(|| Bitmap::all_high_bits(capacity)),
        );
        let data_chunk = DataChunk::new(columns.to_owned(), vis);
        if let Bool(filter_res) = filter
            .eval_infallible(&data_chunk, |err| ctx.on_compute_error(err, identity))
            .as_ref()
        {
            Ok(Some(filter_res.to_bitmap()))
        } else {
            Err(StreamExecutorError::from(anyhow!(
                "Filter can only receive bool array"
            )))
        }
    } else {
        Ok(visibility.cloned())
    }
}

/// State table and column mapping for `MaterializedState` variant of `AggCallState`.
pub struct AggStateTable<S: StateStore> {
    pub table: StateTable<S>,
    pub mapping: StateTableColumnMapping,
}

pub fn for_each_agg_state_table<S: StateStore, F: Fn(&mut AggStateTable<S>)>(
    agg_state_tables: &mut [Option<AggStateTable<S>>],
    f: F,
) {
    agg_state_tables
        .iter_mut()
        .filter_map(Option::as_mut)
        .for_each(|state_table| {
            f(state_table);
        });
}
