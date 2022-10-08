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

use risingwave_common::bail;
use risingwave_pb::expr::expr_node::Type::*;
use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::executor::DynamicFilterExecutor;

pub struct DynamicFilterExecutorBuilder;

impl ExecutorBuilder for DynamicFilterExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::DynamicFilter)?;
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

        // Only write the RHS value if this actor is in charge of vnode 0
        let is_right_table_writer = vnodes.is_set(0);

        let state_table_l =
            StateTable::from_table_catalog(node.get_left_table()?, store.clone(), Some(vnodes));

        let state_table_r = StateTable::from_table_catalog(node.get_right_table()?, store, None);

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
            is_right_table_writer,
            params.executor_stats,
        )))
    }
}
