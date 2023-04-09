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

use std::fmt;

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{Condition, ConditionDisplay};

/// [`Filter`] iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Filter<PlanRef> {
    pub predicate: Condition,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> Filter<PlanRef> {
    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let input_schema = self.input.schema();
        write!(
            f,
            "{} {{ predicate: {} }}",
            name,
            ConditionDisplay {
                condition: &self.predicate,
                input_schema
            }
        )
    }

    pub fn new(predicate: Condition, input: PlanRef) -> Self {
        Filter { predicate, input }
    }
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for Filter<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let mut functional_dependency = self.input.functional_dependency().clone();
        for i in &self.predicate.conjunctions {
            if let Some((col, _)) = i.as_eq_const() {
                functional_dependency.add_constant_columns(&[col.index()])
            } else if let Some((left, right)) = i.as_eq_cond() {
                functional_dependency
                    .add_functional_dependency_by_column_indices(&[left.index()], &[right.index()]);
                functional_dependency
                    .add_functional_dependency_by_column_indices(&[right.index()], &[left.index()]);
            }
        }
        functional_dependency
    }
}

impl<PlanRef> Filter<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.predicate = self.predicate.clone().rewrite_expr(r);
    }
}
