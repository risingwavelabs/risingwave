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
use crate::optimizer::plan_node::{
    ColumnPruningContext, PlanTreeNode, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext, generic,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIntersect` returns the intersect of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIntersect {
    pub base: PlanBase<Logical>,
    core: generic::Intersect<PlanRef>,
}

impl LogicalIntersect {
    pub fn new(all: bool, inputs: Vec<PlanRef>) -> Self {
        assert!(Schema::all_type_eq(inputs.iter().map(|x| x.schema())));
        let core = generic::Intersect { all, inputs };
        let base = PlanBase::new_logical_with_core(&core);
        LogicalIntersect { base, core }
    }

    pub fn create(all: bool, inputs: Vec<PlanRef>) -> PlanRef {
        LogicalIntersect::new(all, inputs).into()
    }

    pub fn all(&self) -> bool {
        self.core.all
    }
}

impl PlanTreeNode for LogicalIntersect {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        self.core.inputs.clone().into_iter().collect()
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(self.all(), inputs.to_vec()).into()
    }
}

impl_distill_by_unit!(LogicalIntersect, core, "LogicalIntersect");

impl ColPrunable for LogicalIntersect {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.prune_col(required_cols, ctx))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ExprRewritable for LogicalIntersect {}

impl ExprVisitable for LogicalIntersect {}

impl PredicatePushdown for LogicalIntersect {
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

impl ToBatch for LogicalIntersect {
    fn to_batch(&self) -> Result<PlanRef> {
        unimplemented!()
    }
}

impl ToStream for LogicalIntersect {
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
