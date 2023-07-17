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
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::UpdateNode;

use super::utils::impl_distill_by_unit;
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchUpdate` implements [`LogicalUpdate`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchUpdate {
    pub base: PlanBase,
    pub logical: generic::Update<PlanRef>,
}

impl BatchUpdate {
    pub fn new(logical: generic::Update<PlanRef>, schema: Schema) -> Self {
        let ctx = logical.input.ctx();
        let base = PlanBase::new_batch(ctx, schema, Distribution::Single, Order::any());
        Self { base, logical }
    }
}

impl PlanTreeNodeUnary for BatchUpdate {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical, self.schema().clone())
    }
}

impl_plan_tree_node_for_unary! { BatchUpdate }
impl_distill_by_unit!(BatchUpdate, logical, "BatchUpdate");

impl ToDistributedBatch for BatchUpdate {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchUpdate {
    fn to_batch_prost_body(&self) -> NodeBody {
        let exprs = self
            .logical
            .exprs
            .iter()
            .map(|x| x.to_expr_proto())
            .collect();

        NodeBody::Update(UpdateNode {
            exprs,
            table_id: self.logical.table_id.table_id(),
            table_version_id: self.logical.table_version_id,
            returning: self.logical.returning,
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
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical, self.schema().clone()).into()
    }
}
