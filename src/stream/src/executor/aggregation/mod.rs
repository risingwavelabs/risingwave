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
pub use agg_state::*;
use anyhow::anyhow;
use risingwave_common::array::column::Column;
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::ActorContextRef;
use crate::common::InfallibleExpression;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::Executor;

mod agg_call;
mod agg_group;
pub mod agg_impl;
mod agg_state;
mod minput;
mod state_cache;
mod value;

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
    columns: &[Column],
    base_visibility: Option<&Bitmap>,
    capacity: usize,
) -> StreamExecutorResult<Option<Bitmap>> {
    let agg_col_vis = if matches!(
        agg_call.kind,
        AggKind::Min | AggKind::Max | AggKind::StringAgg
    ) {
        // should skip NULL value for these kinds of agg function
        let agg_col_idx = agg_call.args.val_indices()[0]; // the first arg is the agg column for all these kinds
        let agg_col_bitmap = columns[agg_col_idx].array_ref().null_bitmap();
        Some(agg_col_bitmap)
    } else {
        None
    };

    let filter_vis = if let Some(ref filter) = agg_call.filter {
        let data_chunk = DataChunk::new(columns.to_vec(), capacity);
        if let Bool(filter_res) = filter
            .eval_infallible(&data_chunk, |err| ctx.on_compute_error(err, identity))
            .as_ref()
        {
            Some(filter_res.to_bitmap())
        } else {
            return Err(StreamExecutorError::from(anyhow!(
                "Filter can only receive bool array"
            )));
        }
    } else {
        None
    };

    let mut res = base_visibility.cloned();
    [agg_col_vis, filter_vis.as_ref()]
        .into_iter()
        .for_each(|bitmap| {
            if let Some(bitmap) = bitmap {
                res = Some(
                    res.as_ref()
                        .map_or_else(|| bitmap.clone(), |res| res & bitmap),
                );
            }
        });
    Ok(res)
}

pub fn iter_table_storage<S>(
    state_storages: &mut [AggStateStorage<S>],
) -> impl Iterator<Item = &mut StateTable<S>>
where
    S: StateStore,
{
    state_storages
        .iter_mut()
        .filter_map(|storage| match storage {
            AggStateStorage::ResultValue => None,
            AggStateStorage::MaterializedInput { table, .. } => Some(table),
        })
}
