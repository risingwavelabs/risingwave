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

use itertools::Itertools;
use risingwave_common::catalog::Schema;

use super::utils::impl_distill_by_unit;
use super::{
    ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PlanTreeNode, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext, generic,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalExcept` returns the rows of its first input except any
///  matching rows from its other inputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalExcept {
    pub base: PlanBase<Logical>,
    core: generic::Except<PlanRef>,
}

impl LogicalExcept {
    pub fn new(all: bool, inputs: Vec<PlanRef>) -> Self {
        assert!(Schema::all_type_eq(inputs.iter().map(|x| x.schema())));
        let core = generic::Except { all, inputs };
        let base = PlanBase::new_logical_with_core(&core);
        LogicalExcept { base, core }
    }

    pub fn create(all: bool, inputs: Vec<PlanRef>) -> PlanRef {
        LogicalExcept::new(all, inputs).into()
    }

    pub fn all(&self) -> bool {
        self.core.all
    }
}

impl PlanTreeNode for LogicalExcept {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        self.core.inputs.clone().into_iter().collect()
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(self.all(), inputs.to_vec()).into()
    }
}

impl_distill_by_unit!(LogicalExcept, core, "LogicalExcept");

impl ColPrunable for LogicalExcept {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.prune_col(required_cols, ctx))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ExprRewritable for LogicalExcept {}

impl ExprVisitable for LogicalExcept {}

impl PredicatePushdown for LogicalExcept {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.predicate_pushdown(predicate.clone(), ctx))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ToBatch for LogicalExcept {
    fn to_batch(&self) -> Result<PlanRef> {
        unimplemented!()
    }
}

impl ToStream for LogicalExcept {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unimplemented!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unimplemented!()
    }
}
