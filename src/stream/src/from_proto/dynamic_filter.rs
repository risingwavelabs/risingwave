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



use risingwave_common::error::{internal_error, Result};

use risingwave_common::types::DataType;
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::{InputRefExpression};

use risingwave_pb::expr::expr_node::Type::*;

use super::*;

use crate::executor::{DynamicFilterExecutor};

pub struct DynamicFilterExecutorBuilder;

impl ExecutorBuilder for DynamicFilterExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::DynamicFilter)?;
        let source_r = params.input.remove(1);
        let source_l = params.input.remove(0);
        let key_l = node.get_left_key() as usize;

        let prost_condition = node.get_condition()?;
        let comparator = prost_condition.get_expr_type()?;
        if !matches!(
            comparator,
            GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual
        ) {
            return Err(internal_error(
                "`DynamicFilterExecutor` only supports comparators:\
                GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual",
            ));
        }

        let condition = new_binary_expr(
            comparator,
            DataType::Boolean,
            Box::new(InputRefExpression::new(
                source_l.schema().data_types()[key_l].clone(),
                0,
            )),
            Box::new(InputRefExpression::new(
                source_r.schema().data_types()[0].clone(),
                1,
            )),
        );

        let _key = source_l.schema().fields[key_l as usize].data_type();

        // TODO: add the tables and backing state store.
        // let left_table_id = TableId::from(node.left_table.as_ref().unwrap().id);
        // let right_table_id = TableId::from(node.right_table.as_ref().unwrap().id);

        Ok(Box::new(DynamicFilterExecutor::new(
            source_l,
            source_r,
            key_l,
            params.pk_indices,
            params.executor_id,
            condition,
            comparator,
            // keyspace_l: Keyspace::table_root(store.clone(), &TableId { table_id: 0 }),
            // keyspace_r: Keyspace::table_root(store, &right_table_id),
            params.actor_id as u64,
            params.executor_stats,
        )))
    }
}
