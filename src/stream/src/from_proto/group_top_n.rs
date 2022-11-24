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

use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::stream_plan::GroupTopNNode;

use super::*;
use crate::cache::LruManagerRef;
use crate::common::table::state_table::StateTable;
use crate::executor::{ActorContextRef, GroupTopNExecutor, PkIndices};

pub struct GroupTopNExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for GroupTopNExecutorBuilder {
    type Node = GroupTopNNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let group_by: Vec<usize> = node
            .get_group_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect();
        let table = node.get_table()?;
        let vnodes = params.vnode_bitmap.map(Arc::new);
        let state_table = StateTable::from_table_catalog(table, store, vnodes).await;
        let order_pairs = table.get_pk().iter().map(OrderPair::from_prost).collect();
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let group_key_types = input.schema().data_types()[..group_by.len()].to_vec();

        let args = GroupTopNExecutorDispatcherArgs {
            input,
            ctx: params.actor_context,
            order_pairs,
            offset_and_limit: (node.offset as usize, node.limit as usize),
            order_by_len: node.order_by_len as usize,
            pk_indices: params.pk_indices,
            executor_id: params.executor_id,
            group_by,
            state_table,
            lru_manager: stream.context.lru_manager.clone(),
            cache_size: 1 << 16,
            with_ties: node.with_ties,
            group_key_types,
        };
        args.dispatch()
    }
}

struct GroupTopNExecutorDispatcherArgs<S: StateStore> {
    input: BoxedExecutor,
    ctx: ActorContextRef,
    order_pairs: Vec<OrderPair>,
    offset_and_limit: (usize, usize),
    order_by_len: usize,
    pk_indices: PkIndices,
    executor_id: u64,
    group_by: Vec<usize>,
    state_table: StateTable<S>,
    lru_manager: Option<LruManagerRef>,
    cache_size: usize,
    with_ties: bool,
    group_key_types: Vec<DataType>,
}

impl<S: StateStore> HashKeyDispatcher for GroupTopNExecutorDispatcherArgs<S> {
    type Output = StreamResult<BoxedExecutor>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        match self.with_ties {
            true => Ok(GroupTopNExecutor::<K, S, true>::new(
                self.input,
                self.ctx,
                self.order_pairs,
                self.offset_and_limit,
                self.order_by_len,
                self.pk_indices,
                self.executor_id,
                self.group_by,
                self.state_table,
                self.lru_manager,
                self.cache_size,
            )?
            .boxed()),
            false => Ok(GroupTopNExecutor::<K, S, false>::new(
                self.input,
                self.ctx,
                self.order_pairs,
                self.offset_and_limit,
                self.order_by_len,
                self.pk_indices,
                self.executor_id,
                self.group_by,
                self.state_table,
                self.lru_manager,
                self.cache_size,
            )?
            .boxed()),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}
