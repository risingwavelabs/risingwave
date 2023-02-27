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

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::utils::Condition;

/// [`Filter`] iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Filter<PlanRef> {
    pub predicate: Condition,
    pub input: PlanRef,
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
}

impl<PlanRef> Filter<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.predicate = self.predicate.clone().rewrite_expr(r);
    }
}
