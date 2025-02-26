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
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail_not_implemented;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use smallvec::{SmallVec, smallvec};

use super::expr_visitable::ExprVisitable;
use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, Logical, PlanBase, PlanTreeNode,
    PredicatePushdown, PredicatePushdownContext, RewriteStreamContext, ToBatch, ToStream,
    ToStreamContext, generic,
};
use crate::PlanRef;
use crate::binder::ShareId;
use crate::error::Result;
use crate::utils::Condition;

/// `LogicalRecursiveUnion` returns the union of the rows of its inputs.
/// note: if `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalRecursiveUnion {
    pub base: PlanBase<Logical>,
    core: generic::RecursiveUnion<PlanRef>,
}

impl LogicalRecursiveUnion {
    pub fn new(base_plan: PlanRef, recursive: PlanRef, id: ShareId) -> Self {
        let core = generic::RecursiveUnion {
            base: base_plan,
            recursive,
            id,
        };
        let base = PlanBase::new_logical_with_core(&core);
        LogicalRecursiveUnion { base, core }
    }

    pub fn create(base_plan: PlanRef, recursive: PlanRef, id: ShareId) -> PlanRef {
        Self::new(base_plan, recursive, id).into()
    }

    pub(super) fn pretty_fields(base: impl GenericPlanRef, name: &str) -> XmlNode<'_> {
        childless_record(name, vec![("id", Pretty::debug(&base.id().0))])
    }
}

impl PlanTreeNode for LogicalRecursiveUnion {
    fn inputs(&self) -> SmallVec<[PlanRef; 2]> {
        smallvec![self.core.base.clone(), self.core.recursive.clone()]
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        let mut inputs = inputs.iter().cloned();
        Self::create(inputs.next().unwrap(), inputs.next().unwrap(), self.core.id)
    }
}

impl Distill for LogicalRecursiveUnion {
    fn distill<'a>(&self) -> XmlNode<'a> {
        Self::pretty_fields(&self.base, "LogicalRecursiveUnion")
    }
}

impl ColPrunable for LogicalRecursiveUnion {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.prune_col(required_cols, ctx))
            .collect_vec();
        let new_plan = self.clone_with_inputs(&new_inputs);
        self.ctx()
            .insert_rcte_cache_plan(self.core.id, new_plan.clone());
        new_plan
    }
}

impl ExprRewritable for LogicalRecursiveUnion {}

impl ExprVisitable for LogicalRecursiveUnion {}

impl PredicatePushdown for LogicalRecursiveUnion {
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
        let new_plan = self.clone_with_inputs(&new_inputs);
        self.ctx()
            .insert_rcte_cache_plan(self.core.id, new_plan.clone());
        new_plan
    }
}

impl ToBatch for LogicalRecursiveUnion {
    fn to_batch(&self) -> Result<PlanRef> {
        bail_not_implemented!(
            issue = 15135,
            "recursive CTE not supported for to_batch of LogicalRecursiveUnion"
        )
    }
}

impl ToStream for LogicalRecursiveUnion {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail_not_implemented!(
            issue = 15135,
            "recursive CTE not supported for to_stream of LogicalRecursiveUnion"
        )
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!(
            issue = 15135,
            "recursive CTE not supported for logical_rewrite_for_stream of LogicalRecursiveUnion"
        )
    }
}
