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

use std::collections::HashMap;

use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_expr::aggregate::{AggCall, AggType, PbAggKind};
use risingwave_expr::expr::{LogReport, NonStrictExpression};
use risingwave_pb::stream_plan::PbAggNodeVersion;

use crate::executor::prelude::*;

mod agg_group;
mod agg_state;
mod agg_state_cache;
mod distinct;
mod hash_agg;
mod minput;
mod simple_agg;
mod stateless_simple_agg;

pub use agg_state::AggStateStorage;
pub use hash_agg::HashAggExecutor;
pub use simple_agg::SimpleAggExecutor;
pub use stateless_simple_agg::StatelessSimpleAggExecutor;

/// Arguments needed to construct an `XxxAggExecutor`.
pub struct AggExecutorArgs<S: StateStore, E: AggExecutorExtraArgs> {
    pub version: PbAggNodeVersion,

    // basic
    pub input: Executor,
    pub actor_ctx: ActorContextRef,
    pub info: ExecutorInfo,

    // system configs
    pub extreme_cache_size: usize,

    // agg common things
    pub agg_calls: Vec<AggCall>,
    pub row_count_index: usize,
    pub storages: Vec<AggStateStorage<S>>,
    pub intermediate_state_table: StateTable<S>,
    pub distinct_dedup_tables: HashMap<usize, StateTable<S>>,
    pub watermark_epoch: AtomicU64Ref,

    // extra
    pub extra: E,
}

pub trait AggExecutorExtraArgs {}

pub struct SimpleAggExecutorExtraArgs {
    pub must_output_per_barrier: bool,
}
impl AggExecutorExtraArgs for SimpleAggExecutorExtraArgs {}

/// Extra arguments needed to construct an `HashAggExecutor`.
pub struct HashAggExecutorExtraArgs {
    pub group_key_indices: Vec<usize>,
    pub chunk_size: usize,
    pub max_dirty_groups_heap_size: usize,
    pub emit_on_window_close: bool,
}
impl AggExecutorExtraArgs for HashAggExecutorExtraArgs {}

async fn agg_call_filter_res(
    agg_call: &AggCall,
    chunk: &DataChunk,
) -> StreamExecutorResult<Bitmap> {
    let mut vis = chunk.visibility().clone();
    if matches!(
        agg_call.agg_type,
        AggType::Builtin(PbAggKind::Min | PbAggKind::Max | PbAggKind::StringAgg)
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

fn iter_table_storage<S>(
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
