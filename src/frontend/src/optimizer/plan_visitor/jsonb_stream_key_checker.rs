// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::FieldDisplay;
use risingwave_common::types::DataType;

use super::{DefaultBehavior, Merge};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{PlanNode, *};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Debug, Clone, Default)]
pub struct StreamKeyChecker;

impl StreamKeyChecker {
    fn visit_inputs(&mut self, plan: &impl PlanNode) -> Option<String> {
        let results = plan.inputs().into_iter().map(|input| self.visit(input));
        Self::default_behavior().apply(results)
    }
}

impl PlanVisitor for StreamKeyChecker {
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
                return Some(format!(
                    "Column {} should not be in the distinct key because it has data type Jsonb",
                    FieldDisplay(&schema[*idx])
                ));
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
                return Some(format!(
                    "Column {} should not be in the TopN group key because it has data type Jsonb",
                    FieldDisplay(&schema[*idx])
                ));
            }
        }
        for idx in plan
            .topn_order()
            .column_orders
            .iter()
            .map(|c| c.column_index)
        {
            if data_types[idx] == DataType::Jsonb {
                return Some(format!(
                    "Column {} should not be in the TopN order key because it has data type Jsonb",
                    FieldDisplay(&schema[idx])
                ));
            }
        }
        self.visit_inputs(plan)
    }

    fn visit_logical_union(&mut self, plan: &LogicalUnion) -> Self::Result {
        for field in &plan.inputs()[0].schema().fields {
            if field.data_type() == DataType::Jsonb {
                return Some(format!(
                    "Column {} should not be in the union because it has data type Jsonb",
                    FieldDisplay(field)
                ));
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
                return Some(format!("Column {} should not be in the aggregation group key because it has data type Jsonb", FieldDisplay(&schema[idx])));
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
                    return Some(format!("Column {} should not be in the over window partition key because it has data type Jsonb", FieldDisplay(&schema[idx])));
                }
            }

            for idx in func.order_by.iter().map(|c| c.column_index) {
                if data_types[idx] == DataType::Jsonb {
                    return Some(format!("Column {} should not be in the over window order by key because it has data type Jsonb", FieldDisplay(&schema[idx])));
                }
            }
        }
        self.visit_inputs(plan)
    }
}
