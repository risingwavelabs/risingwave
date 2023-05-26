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

use std::sync::Arc;

use risingwave_expr::function::window::WindowFuncCall;
use risingwave_pb::stream_plan::PbEowcOverWindowNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::{
    BoxedExecutor, EowcOverWindowExecutor, EowcOverWindowExecutorArgs, Executor,
};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct EowcOverWindowExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for EowcOverWindowExecutorBuilder {
    type Node = PbEowcOverWindowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let calls: Vec<_> = node
            .get_calls()
            .iter()
            .map(WindowFuncCall::from_protobuf)
            .try_collect()?;
        let partition_key_indices = node
            .get_partition_by()
            .iter()
            .map(|i| *i as usize)
            .collect();
        let order_key_index = node.get_order_by()[0].get_column_index() as usize;
        let vnodes = Some(Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for EOWC over window"),
        ));
        let state_table =
            StateTable::from_table_catalog_inconsistent_op(node.get_state_table()?, store, vnodes)
                .await;
        Ok(EowcOverWindowExecutor::new(EowcOverWindowExecutorArgs {
            input,
            actor_ctx: params.actor_context,
            pk_indices: params.pk_indices,
            executor_id: params.executor_id,
            calls,
            partition_key_indices,
            order_key_index,
            state_table,
            watermark_epoch: stream.get_watermark_epoch(),
        })
        .boxed())
    }
}
