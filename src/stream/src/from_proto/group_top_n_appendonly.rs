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

// Copyright 2023 Singularity Data
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

use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::stream_plan::GroupTopNNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::{ActorContextRef, AppendOnlyGroupTopNExecutor};
use crate::task::AtomicU64Ref;

pub struct AppendOnlyGroupTopNExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for AppendOnlyGroupTopNExecutorBuilder {
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
        let storage_key = table
            .get_pk()
            .iter()
            .map(OrderPair::from_protobuf)
            .collect();
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let group_key_types = group_by
            .iter()
            .map(|i| input.schema()[*i].data_type())
            .collect();
        let order_by = node.order_by.iter().map(OrderPair::from_protobuf).collect();

        assert_eq!(&params.pk_indices, input.pk_indices());
        let args = AppendOnlyGroupTopNExecutorDispatcherArgs {
            input,
            ctx: params.actor_context,
            storage_key,
            offset_and_limit: (node.offset as usize, node.limit as usize),
            order_by,
            executor_id: params.executor_id,
            group_by,
            state_table,
            watermark_epoch: stream.get_watermark_epoch(),
            with_ties: node.with_ties,
            group_key_types,
        };
        args.dispatch()
    }
}

struct AppendOnlyGroupTopNExecutorDispatcherArgs<S: StateStore> {
    input: BoxedExecutor,
    ctx: ActorContextRef,
    storage_key: Vec<OrderPair>,
    offset_and_limit: (usize, usize),
    order_by: Vec<OrderPair>,
    executor_id: u64,
    group_by: Vec<usize>,
    state_table: StateTable<S>,
    watermark_epoch: AtomicU64Ref,
    with_ties: bool,
    group_key_types: Vec<DataType>,
}

impl<S: StateStore> HashKeyDispatcher for AppendOnlyGroupTopNExecutorDispatcherArgs<S> {
    type Output = StreamResult<BoxedExecutor>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        match self.with_ties {
            true => Ok(AppendOnlyGroupTopNExecutor::<K, S, true>::new(
                self.input,
                self.ctx,
                self.storage_key,
                self.offset_and_limit,
                self.order_by,
                self.executor_id,
                self.group_by,
                self.state_table,
                self.watermark_epoch,
            )?
            .boxed()),
            false => Ok(AppendOnlyGroupTopNExecutor::<K, S, false>::new(
                self.input,
                self.ctx,
                self.storage_key,
                self.offset_and_limit,
                self.order_by,
                self.executor_id,
                self.group_by,
                self.state_table,
                self.watermark_epoch,
            )?
            .boxed()),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}
