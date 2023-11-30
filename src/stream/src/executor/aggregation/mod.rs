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

pub use agg_group::*;
pub use agg_state::*;
pub use distinct::*;
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_expr::aggregate::{AggCall, AggKind};
use risingwave_expr::expr::{LogReport, NonStrictExpression};
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorResult;

mod agg_group;
mod agg_state;
mod agg_state_cache;
mod distinct;
mod minput;

pub async fn agg_call_filter_res(
    agg_call: &AggCall,
    chunk: &DataChunk,
) -> StreamExecutorResult<Bitmap> {
    let mut vis = chunk.visibility().clone();
    if matches!(
        agg_call.kind,
        AggKind::Min | AggKind::Max | AggKind::StringAgg
    ) {
        // should skip NULL value for these kinds of agg function
        let agg_col_idx = agg_call.args.val_indices()[0]; // the first arg is the agg column for all these kinds
        let agg_col_bitmap = chunk.column_at(agg_col_idx).null_bitmap();
        vis &= agg_col_bitmap;
    }

    if let Some(ref filter) = agg_call.filter {
        // TODO: should we build `filter` in non-strict mode?
        if let Bool(filter_res) = NonStrictExpression::new_topmost(&**filter, LogReport)
            .eval_infallible(chunk)
            .await
            .as_ref()
        {
            vis &= filter_res.to_bitmap();
        } else {
            bail!("Filter can only receive bool array");
        }
    }

    Ok(vis)
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
            AggStateStorage::Value => None,
            AggStateStorage::MaterializedInput { table, .. } => Some(table),
        })
}
