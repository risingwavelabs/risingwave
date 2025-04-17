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
use risingwave_pb::batch_plan::ProjectSetNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, generic};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    PlanBase, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchProjectSet {
    pub base: PlanBase<Batch>,
    core: generic::ProjectSet<PlanRef>,
}

impl BatchProjectSet {
    pub fn new(core: generic::ProjectSet<PlanRef>) -> Self {
        let distribution = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(core.input.distribution());

        let base =
            PlanBase::new_batch_with_core(&core, distribution, core.get_out_column_index_order());
        BatchProjectSet { base, core }
    }
}

impl_distill_by_unit!(BatchProjectSet, core, "BatchProjectSet");

impl PlanTreeNodeUnary for BatchProjectSet {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { BatchProjectSet }

impl ToDistributedBatch for BatchProjectSet {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }

    // TODO: implement to_distributed_with_required like BatchProject
}

impl ToBatchPb for BatchProjectSet {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::ProjectSet(ProjectSetNode {
            select_list: self
                .core
                .select_list
                .iter()
                .map(|select_item| select_item.to_project_set_select_item_proto())
                .collect_vec(),
        })
    }
}

impl ToLocalBatch for BatchProjectSet {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchProjectSet {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for BatchProjectSet {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
