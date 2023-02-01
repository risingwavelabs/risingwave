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

use risingwave_common::bail;
use risingwave_pb::expr::expr_node::Type::{
    GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
};
use risingwave_pb::stream_plan::DynamicFilterNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::DynamicFilterExecutor;

pub struct DynamicFilterExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for DynamicFilterExecutorBuilder {
    type Node = DynamicFilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();
        let key_l = node.get_left_key() as usize;

        let vnodes = Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for dynamic filter"),
        );

        let prost_condition = node.get_condition()?;
        let comparator = prost_condition.get_expr_type()?;
        if !matches!(
            comparator,
            GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual
        ) {
            bail!(
                "`DynamicFilterExecutor` only supports comparators:\
                GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual",
            );
        }

        // TODO: use consistent operation for dynamic filter <https://github.com/risingwavelabs/risingwave/issues/3893>
        let state_table_l = StateTable::from_table_catalog_inconsistent_op(
            node.get_left_table()?,
            store.clone(),
            Some(vnodes),
        )
        .await;

        let state_table_r =
            StateTable::from_table_catalog_inconsistent_op(node.get_right_table()?, store, None)
                .await;

        Ok(Box::new(DynamicFilterExecutor::new(
            params.actor_context,
            source_l,
            source_r,
            key_l,
            params.pk_indices,
            params.executor_id,
            comparator,
            state_table_l,
            state_table_r,
            params.executor_stats,
            params.env.config().developer.stream_chunk_size,
        )))
    }
}
