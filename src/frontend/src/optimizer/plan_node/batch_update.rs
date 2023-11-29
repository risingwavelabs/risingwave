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

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::UpdateNode;

use super::batch::prelude::*;
use super::generic::GenericPlanRef;
use super::utils::impl_distill_by_unit;
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchUpdate` implements [`super::LogicalUpdate`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchUpdate {
    pub base: PlanBase<Batch>,
    pub core: generic::Update<PlanRef>,
}

impl BatchUpdate {
    pub fn new(core: generic::Update<PlanRef>, schema: Schema) -> Self {
        assert_eq!(core.input.distribution(), &Distribution::Single);
        let ctx = core.input.ctx();
        let base = PlanBase::new_batch(ctx, schema, Distribution::Single, Order::any());
        Self { base, core }
    }
}

impl PlanTreeNodeUnary for BatchUpdate {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core, self.schema().clone())
    }
}

impl_plan_tree_node_for_unary! { BatchUpdate }
impl_distill_by_unit!(BatchUpdate, core, "BatchUpdate");

impl ToDistributedBatch for BatchUpdate {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchUpdate {
    fn to_batch_prost_body(&self) -> NodeBody {
        let exprs = self.core.exprs.iter().map(|x| x.to_expr_proto()).collect();

        let update_column_indices = self
            .core
            .update_column_indices
            .iter()
            .map(|i| *i as _)
            .collect_vec();
        NodeBody::Update(UpdateNode {
            exprs,
            table_id: self.core.table_id.table_id(),
            table_version_id: self.core.table_version_id,
            returning: self.core.returning,
            update_column_indices,
        })
    }
}

impl ToLocalBatch for BatchUpdate {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchUpdate {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.schema().clone()).into()
    }
}

impl ExprVisitable for BatchUpdate {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.exprs.iter().for_each(|e| v.visit_expr(e));
    }
}
