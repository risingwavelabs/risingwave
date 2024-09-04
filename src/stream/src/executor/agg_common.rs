// Copyright 2024 RisingWave Labs
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

use risingwave_expr::aggregate::AggCall;
use risingwave_pb::stream_plan::PbAggNodeVersion;

use super::aggregation::AggStateStorage;
use crate::executor::prelude::*;

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
