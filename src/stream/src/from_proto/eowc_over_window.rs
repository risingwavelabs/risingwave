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

use std::sync::Arc;

use risingwave_expr::window_function::WindowFuncCall;
use risingwave_pb::stream_plan::PbEowcOverWindowNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::{StateTableBuilder, StateTableOpConsistencyLevel};
use crate::error::StreamResult;
use crate::executor::{EowcOverWindowExecutor, EowcOverWindowExecutorArgs, Executor};
use crate::task::ExecutorParams;

pub struct EowcOverWindowExecutorBuilder;

impl ExecutorBuilder for EowcOverWindowExecutorBuilder {
    type Node = PbEowcOverWindowNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
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
        let state_table = StateTableBuilder::new(node.get_state_table()?, store, vnodes)
            .with_op_consistency_level(StateTableOpConsistencyLevel::Inconsistent)
            .enable_preload_all_rows_by_config(&params.actor_context.config)
            .build()
            .await;
        let exec = EowcOverWindowExecutor::new(EowcOverWindowExecutorArgs {
            actor_ctx: params.actor_context,

            input,

            schema: params.info.schema.clone(),
            calls,
            partition_key_indices,
            order_key_index,
            state_table,
            watermark_epoch: params.watermark_epoch,
        });
        Ok((params.info, exec).into())
    }
}
