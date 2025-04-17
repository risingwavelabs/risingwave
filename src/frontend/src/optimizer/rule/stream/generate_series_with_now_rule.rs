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

use risingwave_common::types::DataType;
use risingwave_pb::expr::table_function::PbType as PbTableFuncType;

use crate::PlanRef;
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::{LogicalNow, generic};
use crate::optimizer::rule::{BoxedRule, Rule};

pub struct GenerateSeriesWithNowRule {}
impl Rule for GenerateSeriesWithNowRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let ctx = plan.ctx();
        let table_func = plan.as_logical_table_function()?.table_function();

        if !table_func.args.iter().any(|arg| arg.has_now()) {
            return None;
        }

        if !(table_func.function_type == PbTableFuncType::GenerateSeries
            && table_func.args.len() == 3
            && table_func.args[0].return_type() == DataType::Timestamptz
            && table_func.args[1].is_now()
            && table_func.args[2].return_type() == DataType::Interval)
        {
            // only convert `generate_series(const timestamptz, now(), const interval)`
            ctx.warn_to_user(
                "`now()` is currently only supported in `generate_series(timestamptz, timestamptz, interval)` function as `stop`. \
                You may not using it correctly. Please kindly check the document."
            );
            return None;
        }

        let start_timestamp = ctx
            .session_timezone()
            .rewrite_expr(table_func.args[0].clone())
            .try_fold_const()
            .transpose()
            .ok()
            .flatten()
            .flatten();
        let interval = ctx
            .session_timezone()
            .rewrite_expr(table_func.args[2].clone())
            .try_fold_const()
            .transpose()
            .ok()
            .flatten()
            .flatten();

        if start_timestamp.is_none() || interval.is_none() {
            ctx.warn_to_user(
                "When using `generate_series` with `now()`, the `start` and `step` must be non-NULL constants",
            );
            return None;
        }

        Some(
            LogicalNow::new(generic::Now::generate_series(
                ctx,
                start_timestamp.unwrap().into_timestamptz(),
                interval.unwrap().into_interval(),
            ))
            .into(),
        )
    }
}

impl GenerateSeriesWithNowRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}
