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
use std::sync::Arc;

use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};

use super::*;
use crate::executor::aggregation::AggCall;
use crate::executor::{HashAggExecutor, PkIndices};

struct HashAggExecutorDispatcher<S: StateStore>(PhantomData<S>);

struct HashAggExecutorDispatcherArgs<S: StateStore> {
    input: BoxedExecutor,
    agg_calls: Vec<AggCall>,
    key_indices: Vec<usize>,
    keyspace: Vec<Keyspace<S>>,
    pk_indices: PkIndices,
    executor_id: u64,
    vnodes: Arc<Bitmap>,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcher<S> {
    type Input = HashAggExecutorDispatcherArgs<S>;
    type Output = Result<BoxedExecutor>;

    fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new(
            args.input,
            args.agg_calls,
            args.keyspace,
            args.pk_indices,
            args.executor_id,
            args.key_indices,
            Some(args.vnodes),
        )?
        .boxed())
    }
}

pub struct HashAggExecutorBuilder;

impl ExecutorBuilder for HashAggExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::HashAgg)?;
        dbg!(&node);
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.is_append_only, agg_call))
            .try_collect()?;
        // Build vector of keyspace via table ids.
        // One keyspace for one agg call.
        let keyspace = node
            .internal_tables
            .iter()
            .map(|table| Keyspace::table_root(store.clone(), &TableId::new(table.id)))
            .collect();
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
            vnodes: params.vnode_bitmap.expect("vnodes not set").into(),
        };
        HashAggExecutorDispatcher::dispatch_by_kind(kind, args)
    }
}
