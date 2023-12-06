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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprVisitor};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalProject, LogicalValues};
use crate::optimizer::plan_visitor::ExprCorrelatedIdFinder;
use crate::optimizer::PlanRef;

pub struct ValuesExtractProjectRule {}
impl Rule for ValuesExtractProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let old_values: &LogicalValues = plan.as_logical_values()?;

        let mut expr_correlated_id_finder = ExprCorrelatedIdFinder::default();

        if old_values.rows().len() != 1 {
            return None;
        }

        old_values.rows()[0]
            .iter()
            .for_each(|expr| expr_correlated_id_finder.visit_expr(expr));

        if !expr_correlated_id_finder.has_correlated_input_ref() {
            return None;
        }

        let new_values = LogicalValues::create(
            vec![vec![ExprImpl::literal_bigint(1)]],
            Schema::new(vec![Field::with_name(DataType::Int64, "$const")]),
            old_values.ctx(),
        );

        Some(LogicalProject::create(
            new_values,
            old_values.rows()[0].clone(),
        ))
    }
}

impl ValuesExtractProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(ValuesExtractProjectRule {})
    }
}
