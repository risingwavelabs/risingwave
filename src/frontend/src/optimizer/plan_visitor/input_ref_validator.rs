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

use paste::paste;
use risingwave_common::catalog::{Field, Schema};

use crate::expr::ExprVisitor;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{Explain, PlanRef, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanVisitor;

struct ExprVis<'a> {
    schema: &'a Schema,
}

impl ExprVisitor<Option<String>> for ExprVis<'_> {
    fn visit_input_ref(&mut self, input_ref: &crate::expr::InputRef) -> Option<String> {
        if input_ref.data_type != self.schema[input_ref.index].data_type {
            Some(format!(
                "InputRef#{} has type {}, but its type is {} in the input schema",
                input_ref.index, input_ref.data_type, self.schema[input_ref.index].data_type
            ))
        } else {
            None
        }
    }

    fn merge(a: Option<String>, b: Option<String>) -> Option<String> {
        a.or(b)
    }
}

/// Validates that input references are consistent with the input schema.
///
/// Use `InputRefValidator::validate` as an assertion.
#[derive(Debug, Clone, Default)]
pub struct InputRefValidator;

impl InputRefValidator {
    #[track_caller]
    pub fn validate(mut self, plan: PlanRef) {
        if let Some(err) = self.visit(plan.clone()) {
            panic!(
                "Input references are inconsistent with the input schema: {}, plan:\n{}",
                err,
                plan.explain_to_string().expect("failed to explain")
            );
        }
    }
}

macro_rules! visit_filter {
    ($($convention:ident),*) => {
        $(
            paste! {
                fn [<visit_ $convention _filter>](&mut self, plan: &crate::optimizer::plan_node:: [<$convention:camel Filter>]) -> Option<String> {
                    let input = plan.input();
                    let mut vis = ExprVis {
                        schema: input.schema(),
                    };
                    plan.predicate().visit_expr(&mut vis).or_else(|| {
                        self.visit(input)
                    })
                }
            }
        )*
    };
}

macro_rules! visit_project {
    ($($convention:ident),*) => {
        $(
            paste! {
                fn [<visit_ $convention _project>](&mut self, plan: &crate::optimizer::plan_node:: [<$convention:camel Project>]) -> Option<String> {
                    let input = plan.input();
                    let mut vis = ExprVis {
                        schema: input.schema(),
                    };
                    for expr in plan.exprs() {
                        let res = vis.visit_expr(expr);
                        if res.is_some() {
                            return res;
                        }
                    }
                    self.visit(input)
                }
            }
        )*
    };
}

impl PlanVisitor<Option<String>> for InputRefValidator {
    visit_filter!(logical, batch, stream);

    visit_project!(logical, batch, stream);

    fn visit_logical_scan(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalScan,
    ) -> Option<String> {
        let fields = plan
            .table_desc()
            .columns
            .iter()
            .map(|col| Field::from_with_table_name_prefix(col, plan.table_name()))
            .collect();
        let input_schema = Schema { fields };
        let mut vis = ExprVis {
            schema: &input_schema,
        };
        plan.predicate().visit_expr(&mut vis)
    }

    // TODO: add more checks

    fn merge(a: Option<String>, b: Option<String>) -> Option<String> {
        a.or(b)
    }
}
