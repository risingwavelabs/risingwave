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

use risingwave_common::catalog::{Field, FieldDisplay};
use risingwave_common::types::DataType;

use super::{DefaultBehavior, LogicalPlanVisitor, Merge};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct StreamKeyChecker;

impl StreamKeyChecker {
    fn visit_inputs(&mut self, plan: &impl LogicalPlanNode) -> Option<String> {
        let results = plan.inputs().into_iter().map(|input| self.visit(input));
        Self::default_behavior().apply(results)
    }

    fn err_msg(target: &str, field: &Field) -> String {
        format!(
            "JSONB column \"{}\" should not be in the {}.",
            target,
            FieldDisplay(field)
        )
    }
}

impl LogicalPlanVisitor for StreamKeyChecker {
    type Result = Option<String>;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: Option<String>, b| a.or(b))
    }

    fn visit_logical_dedup(&mut self, plan: &LogicalDedup) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();
        for idx in plan.dedup_cols() {
            if data_types[*idx] == DataType::Jsonb {
                return Some(StreamKeyChecker::err_msg("distinct key", &schema[*idx]));
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_top_n(&mut self, plan: &LogicalTopN) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();
        for idx in plan.group_key() {
            if data_types[*idx] == DataType::Jsonb {
                return Some(StreamKeyChecker::err_msg("TopN group key", &schema[*idx]));
            }
        }
        for idx in plan
            .topn_order()
            .column_orders
            .iter()
            .map(|c| c.column_index)
        {
            if data_types[idx] == DataType::Jsonb {
                return Some(StreamKeyChecker::err_msg("TopN order key", &schema[idx]));
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_union(&mut self, plan: &LogicalUnion) -> Self::Result {
        if !plan.all() {
            for field in &plan.inputs()[0].schema().fields {
                if field.data_type() == DataType::Jsonb {
                    return Some(StreamKeyChecker::err_msg("field", field));
                }
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();
        for idx in plan.group_key().indices() {
            if data_types[idx] == DataType::Jsonb {
                return Some(StreamKeyChecker::err_msg(
                    "aggregation group key",
                    &schema[idx],
                ));
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_over_window(&mut self, plan: &LogicalOverWindow) -> Self::Result {
        let input = plan.input();
        let schema = input.schema();
        let data_types = schema.data_types();

        for func in plan.window_functions() {
            for idx in func.partition_by.iter().map(|e| e.index()) {
                if data_types[idx] == DataType::Jsonb {
                    return Some(StreamKeyChecker::err_msg(
                        "over window partition key",
                        &schema[idx],
                    ));
                }
            }

            for idx in func.order_by.iter().map(|c| c.column_index) {
                if data_types[idx] == DataType::Jsonb {
                    return Some(StreamKeyChecker::err_msg(
                        "over window order by key",
                        &schema[idx],
                    ));
                }
            }
        }
        self.visit_inputs(plan)
    }
}
