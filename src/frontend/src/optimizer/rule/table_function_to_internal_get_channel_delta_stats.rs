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

use std::rc::Rc;

use anyhow::bail;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::TableFunctionType;
use crate::optimizer::OptimizerContext;
use crate::optimizer::plan_node::{Logical, LogicalGetChannelDeltaStats, LogicalTableFunction};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

/// Helper function to extract a constant u64 value from an `ExprImpl`.
/// Returns `Ok(Some(value))` if the expression can be folded to a constant u64,
/// `Ok(None)` if the expression is null, or an error if folding fails.
fn expr_impl_to_u64_fn(arg: &crate::expr::ExprImpl) -> anyhow::Result<Option<u64>> {
    match arg.try_fold_const() {
        Some(Ok(value)) => {
            let Some(scalar) = value else {
                return Ok(None);
            };
            match scalar {
                ScalarImpl::Int16(value) => Ok(Some(value as u64)),
                ScalarImpl::Int32(value) => Ok(Some(value as u64)),
                ScalarImpl::Int64(value) => Ok(Some(value as u64)),
                _ => Err(anyhow::anyhow!(
                    "Expected int16, int32, or int64, got {:?}",
                    scalar
                )),
            }
        }
        Some(Err(err)) => Err(anyhow::anyhow!("Failed to fold constant: {}", err)),
        None => Err(anyhow::anyhow!("Expression must be a constant value")),
    }
}

/// Transform the `internal_get_channel_delta_stats()` table function
/// into a plan graph which will return channel statistics from the dashboard API.
/// It will return channel stats with `upstream_fragment_id` and `downstream_fragment_id` as primary key.
pub struct TableFunctionToInternalGetChannelDeltaStatsRule {}
impl FallibleRule<Logical> for TableFunctionToInternalGetChannelDeltaStatsRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::InternalGetChannelDeltaStats
        {
            return ApplyResult::NotApplicable;
        }
        let plan = Self::build_plan(plan.ctx(), &logical_table_function.table_function)?;
        ApplyResult::Ok(plan)
    }
}

impl TableFunctionToInternalGetChannelDeltaStatsRule {
    fn build_plan(
        ctx: Rc<OptimizerContext>,
        table_function: &crate::expr::TableFunction,
    ) -> anyhow::Result<PlanRef> {
        // For now, we'll return empty values since we need to integrate with the dashboard API
        // In a real implementation, this would call the dashboard API to get channel stats
        let fields = vec![
            Field::new("upstream_fragment_id", DataType::Int32),
            Field::new("downstream_fragment_id", DataType::Int32),
            Field::new("upstream_actor_count", DataType::Int32),
            Field::new("backpressure_rate", DataType::Float64),
            Field::new("recv_throughput", DataType::Float64),
            Field::new("send_throughput", DataType::Float64),
        ];

        // Extract parameters if provided
        let (at_time, time_offset) = match table_function.args.len() {
            0 => (None, None), // No default, let the service handle it
            2 => {
                let at_expr = &table_function.args[0];
                let offset_expr = &table_function.args[1];

                let at_time = expr_impl_to_u64_fn(at_expr)?;
                let time_offset = expr_impl_to_u64_fn(offset_expr)?;

                (at_time, time_offset)
            }
            _ => {
                bail!("internal_get_channel_delta_stats expects 0 or 2 arguments");
            }
        };

        // Create a LogicalGetChannelDeltaStats node with the extracted parameters
        let plan = LogicalGetChannelDeltaStats::new(
            ctx.clone(),
            Schema::new(fields),
            at_time,
            time_offset,
        );
        Ok(plan.into())
    }
}

impl TableFunctionToInternalGetChannelDeltaStatsRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalGetChannelDeltaStatsRule {})
    }
}
