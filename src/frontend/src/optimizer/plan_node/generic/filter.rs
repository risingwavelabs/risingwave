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

use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::Schema;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{Condition, ConditionDisplay};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Filter<PlanRef> {
    pub predicate: Condition,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> DistillUnit for Filter<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        let input_schema = self.input.schema();
        let predicate = ConditionDisplay {
            condition: &self.predicate,
            input_schema,
        };
        childless_record(name, vec![("predicate", Pretty::display(&predicate))])
    }
}

impl<PlanRef: GenericPlanRef> Filter<PlanRef> {
    pub fn new(predicate: Condition, input: PlanRef) -> Self {
        Filter { predicate, input }
    }
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for Filter<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.input.stream_key()?.to_vec())
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

    pub(crate) fn visit_exprs(&self, r: &mut dyn ExprVisitor) {
        self.predicate.visit_expr(r);
    }
}
