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

use risingwave_common::bail_not_implemented;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::expr_visitable::ExprVisitable;
use super::utils::impl_distill_by_unit;
use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, Logical, LogicalProject, PlanBase,
    PredicatePushdown, PredicatePushdownContext, RewriteStreamContext, ToBatch, ToStream,
    ToStreamContext, generic,
};
use crate::PlanRef;
use crate::binder::ShareId;
use crate::error::Result;
use crate::utils::Condition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalCteRef {
    pub base: PlanBase<Logical>,
    core: generic::CteRef<PlanRef>,
}

impl LogicalCteRef {
    pub fn new(share_id: ShareId, base_plan: PlanRef) -> Self {
        let core = generic::CteRef::new(share_id, base_plan);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub fn create(share_id: ShareId, base_plan: PlanRef) -> PlanRef {
        Self::new(share_id, base_plan).into()
    }
}

impl_plan_tree_node_for_leaf! {LogicalCteRef}

impl_distill_by_unit! {LogicalCteRef, core, "LogicalCteRef"}

impl ExprRewritable for LogicalCteRef {}

impl ExprVisitable for LogicalCteRef {}

impl ColPrunable for LogicalCteRef {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().copied()).into()
    }
}

impl PredicatePushdown for LogicalCteRef {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatch for LogicalCteRef {
    fn to_batch(&self) -> Result<PlanRef> {
        bail_not_implemented!(
            issue = 15135,
            "recursive CTE not supported for to_batch of LogicalCteRef"
        )
    }
}

impl ToStream for LogicalCteRef {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail_not_implemented!(
            issue = 15135,
            "recursive CTE not supported for to_stream of LogicalCteRef"
        )
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!(
            issue = 15135,
            "recursive CTE not supported for logical_rewrite_for_stream of LogicalCteRef"
        )
    }
}
