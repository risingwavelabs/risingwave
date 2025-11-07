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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::gap_fill::FillStrategy;
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::GapFillNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::common::table::state_table::StateTableBuilder;
use crate::error::StreamResult;
use crate::executor::{Executor, GapFillExecutor, GapFillExecutorArgs};
use crate::task::ExecutorParams;

pub struct GapFillExecutorBuilder;

impl ExecutorBuilder for GapFillExecutorBuilder {
    type Node = GapFillNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &GapFillNode,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let time_column_index = node.get_time_column_index() as usize;

        // Parse interval from ExprNode
        let interval_expr_node = node.get_interval()?;
        let interval_expr =
            build_non_strict_from_prost(interval_expr_node, params.eval_error_report)?;

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

        let fill_columns_with_strategies: HashMap<usize, FillStrategy> =
            fill_columns.into_iter().zip_eq(fill_strategies).collect();

        let state_table =
            StateTableBuilder::new(node.get_state_table().as_ref().unwrap(), store, None)
                .forbid_preload_all_rows()
                .build()
                .await;

        let exec = GapFillExecutor::new(GapFillExecutorArgs {
            ctx: params.actor_context,
            input,
            schema: params.info.schema.clone(),
            chunk_size: params.env.config().developer.chunk_size,
            time_column_index,
            fill_columns: fill_columns_with_strategies,
            gap_interval: interval_expr,
            state_table,
        });

        Ok((params.info, exec).into())
    }
}
