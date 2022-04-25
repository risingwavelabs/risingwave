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

//! Global Streaming Hash Aggregators

use std::marker::PhantomData;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{ExecutorBuilder, PkIndices};
use crate::executor_v2::aggregation::AggCall;
use crate::executor_v2::{BoxedExecutor, Executor, HashAggExecutor};
use crate::task::{build_agg_call_from_prost, ExecutorParams, LocalStreamManagerCore};

struct HashAggExecutorDispatcher<S: StateStore>(PhantomData<S>);

struct HashAggExecutorDispatcherArgs<S: StateStore> {
    input: BoxedExecutor,
    agg_calls: Vec<AggCall>,
    key_indices: Vec<usize>,
    keyspace: Keyspace<S>,
    pk_indices: PkIndices,
    executor_id: u64,
    op_info: String,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcher<S> {
    type Input = HashAggExecutorDispatcherArgs<S>;
    type Output = Result<BoxedExecutor>;

    fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new_from_v1(
            args.input,
            args.agg_calls,
            args.key_indices,
            args.keyspace,
            args.pk_indices,
            args.executor_id,
            args.op_info,
        )?
        .boxed())
    }
}

pub struct HashAggExecutorBuilder;

impl ExecutorBuilder for HashAggExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::HashAggNode)?;
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(build_agg_call_from_prost)
            .try_collect()?;
        let keyspace = Keyspace::shared_executor_root(store, params.operator_id);
        let input = params.input.remove(0);
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].data_type())
            .collect_vec();
        let kind = calc_hash_key_kind(&keys);
        let args = HashAggExecutorDispatcherArgs {
            input,
            agg_calls,
            key_indices,
            keyspace,
            pk_indices: params.pk_indices,
            executor_id: params.executor_id,
            op_info: params.op_info,
        };
        HashAggExecutorDispatcher::dispatch_by_kind(kind, args)
    }
}
