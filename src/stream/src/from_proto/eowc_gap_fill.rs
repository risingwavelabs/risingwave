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

use risingwave_common::gap_fill_types::FillStrategy;
use risingwave_expr::expr::{NonStrictExpression, build_from_prost};
use risingwave_pb::stream_plan::EowcGapFillNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::Executor;
use crate::executor::eowc::{EowcGapFillExecutor, EowcGapFillExecutorArgs};
use crate::task::ExecutorParams;

pub struct EowcGapFillExecutorBuilder;

impl ExecutorBuilder for EowcGapFillExecutorBuilder {
    type Node = EowcGapFillNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &EowcGapFillNode,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let time_column_index = node.get_time_column_index() as usize;

        // Parse interval from ExprNode
        let interval_expr_node = node.get_interval()?;
        let interval_expr = build_from_prost(interval_expr_node)?;
        let gap_interval = NonStrictExpression::for_test(interval_expr);

        let fill_columns: Vec<usize> = node
            .get_fill_columns()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let fill_strategies: Vec<FillStrategy> = node
            .get_fill_strategies()
            .iter()
            .map(|s| match s.as_str() {
                "locf" => Ok(FillStrategy::Locf),
                "interpolate" => Ok(FillStrategy::Interpolate),
                "null" => Ok(FillStrategy::Null),
                _ => anyhow::bail!("unknown fill strategy: {}", s),
            })
            .collect::<anyhow::Result<_>>()?;

        let fill_columns_with_strategies: Vec<(usize, FillStrategy)> =
            fill_columns.into_iter().zip(fill_strategies).collect();

        let vnodes = params.vnode_bitmap.map(|bitmap| Arc::new(bitmap));

        let buffer_table = StateTableBuilder::new(
            node.get_buffer_table().as_ref().unwrap(),
            store.clone(),
            vnodes.clone(),
        )
        .forbid_preload_all_rows()
        .build()
        .await;

        let prev_row_table =
            StateTableBuilder::new(node.get_prev_row_table().as_ref().unwrap(), store, vnodes)
                .forbid_preload_all_rows()
                .build()
                .await;

        let exec = EowcGapFillExecutor::new(EowcGapFillExecutorArgs {
            actor_ctx: params.actor_context,
            input,
            schema: params.info.schema.clone(),
            buffer_table,
            prev_row_table,
            chunk_size: 1024, // TODO: make this configurable
            time_column_index,
            fill_columns: fill_columns_with_strategies,
            gap_interval,
        });

        Ok((params.info, exec).into())
    }
}
