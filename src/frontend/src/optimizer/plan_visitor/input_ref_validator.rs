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

use paste::paste;
use risingwave_common::catalog::{Field, Schema};

use super::{BatchPlanVisitor, DefaultBehavior, LogicalPlanVisitor, Merge, StreamPlanVisitor};
use crate::expr::ExprVisitor;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{ConventionMarker, Explain, PlanRef, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanVisitor;

struct ExprVis<'a> {
    schema: &'a Schema,
    string: Option<String>,
}

impl ExprVisitor for ExprVis<'_> {
    fn visit_input_ref(&mut self, input_ref: &crate::expr::InputRef) {
        if !input_ref
            .data_type
            .equals_datatype(&self.schema[input_ref.index].data_type)
        {
            self.string.replace(format!(
                "InputRef#{} has type {}, but its type is {} in the input schema",
                input_ref.index, input_ref.data_type, self.schema[input_ref.index].data_type
            ));
        }
    }
}

/// Validates that input references are consistent with the input schema.
///
/// Use `InputRefValidator::validate` as an assertion.
#[derive(Debug, Clone, Default)]
pub struct InputRefValidator;

impl InputRefValidator {
    #[track_caller]
    pub fn validate<C: ConventionMarker>(mut self, plan: PlanRef<C>)
    where
        Self: PlanVisitor<C, Result = Option<String>>,
    {
        if let Some(err) = self.visit(plan.clone()) {
            panic!(
                "Input references are inconsistent with the input schema: {}, plan:\n{}",
                err,
                plan.explain_to_string()
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
                        string: None,
                    };
                    plan.predicate().visit_expr(&mut vis);
                    vis.string.or_else(|| {
                        self.[<visit_$convention>](input)
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
                        string: None,
                    };
                    for expr in plan.exprs() {
                        vis.visit_expr(expr);
                        if vis.string.is_some() {
                            return vis.string;
                        }
                    }
                    self.[<visit_$convention>](input)
                }
            }
        )*
    };
}

impl StreamPlanVisitor for InputRefValidator {
    type Result = Option<String>;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    visit_filter!(stream);

    visit_project!(stream);

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: Option<String>, b| a.or(b))
    }
}

impl BatchPlanVisitor for InputRefValidator {
    type Result = Option<String>;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    visit_filter!(batch);

    visit_project!(batch);

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: Option<String>, b| a.or(b))
    }
}

impl LogicalPlanVisitor for InputRefValidator {
    type Result = Option<String>;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    visit_filter!(logical);

    visit_project!(logical);

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a: Option<String>, b| a.or(b))
    }

    fn visit_logical_scan(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalScan,
    ) -> Option<String> {
        let fields = plan
            .table()
            .columns
            .iter()
            .map(|col| Field::from_with_table_name_prefix(col, plan.table_name()))
            .collect();
        let input_schema = Schema { fields };
        let mut vis = ExprVis {
            schema: &input_schema,
            string: None,
        };
        plan.predicate().visit_expr(&mut vis);
        vis.string
    }

    // TODO: add more checks
}
