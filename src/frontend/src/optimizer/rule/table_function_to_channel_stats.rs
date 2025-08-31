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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::prelude::{PlanRef, *};
use crate::expr::TableFunctionType;
use crate::optimizer::OptimizerContext;
use crate::optimizer::plan_node::{Logical, LogicalTableFunction, LogicalValues};
use crate::optimizer::rule::{ApplyResult, FallibleRule};

/// Transform the channel stats table function into a plan that calls the meta service.
/// This is a placeholder implementation that returns empty results.
/// The actual implementation would need to be done in the stream executor.
pub struct TableFunctionToChannelStatsRule {}

impl FallibleRule<Logical> for TableFunctionToChannelStatsRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        match logical_table_function.table_function.function_type {
            TableFunctionType::InternalGetChannelStats => {
                let plan = Self::build_plan(plan.ctx())?;
                ApplyResult::Ok(plan)
            }
            _ => ApplyResult::NotApplicable,
        }
    }
}

impl TableFunctionToChannelStatsRule {
    fn build_plan(ctx: Rc<OptimizerContext>) -> anyhow::Result<PlanRef> {
        let fields = vec![
            Field::new("upstream_fragment_id", DataType::Int32),
            Field::new("downstream_fragment_id", DataType::Int32),
            Field::new("actor_count", DataType::Int32),
            Field::new("backpressure_rate", DataType::Float64),
            Field::new("recv_throughput", DataType::Float64),
            Field::new("send_throughput", DataType::Float64),
        ];

        // For now, return empty results. The actual implementation would need to be done
        // in the stream executor to call the meta service.
        let plan = LogicalValues::new(vec![], Schema::new(fields), ctx.clone());
        Ok(plan.into())
    }

    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToChannelStatsRule {})
    }
}
