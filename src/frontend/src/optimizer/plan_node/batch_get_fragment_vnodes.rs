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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeLeaf, ToBatchPb,
    ToDistributedBatch,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order};

/// `BatchGetFragmentVnodes` represents a batch plan node that retrieves fragment vnode
/// assignments from the meta service.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchGetFragmentVnodes {
    pub base: PlanBase<Batch>,
    pub fragment_id: u32,
}

impl PlanTreeNodeLeaf for BatchGetFragmentVnodes {}
impl_plan_tree_node_for_leaf! { Batch, BatchGetFragmentVnodes }

impl BatchGetFragmentVnodes {
    pub fn new(ctx: crate::OptimizerContextRef, schema: Schema, fragment_id: u32) -> Self {
        Self::with_dist(ctx, schema, fragment_id, Distribution::Single)
    }

    pub fn with_dist(
        ctx: crate::OptimizerContextRef,
        schema: Schema,
        fragment_id: u32,
        dist: Distribution,
    ) -> Self {
        let base = PlanBase::new_batch(ctx, schema, dist, Order::any());
        Self { base, fragment_id }
    }
}

impl Distill for BatchGetFragmentVnodes {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("fragment_id", Pretty::debug(&self.fragment_id))];
        childless_record("BatchGetFragmentVnodes", fields)
    }
}

impl ToDistributedBatch for BatchGetFragmentVnodes {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(
            self.base.ctx(),
            self.base.schema().clone(),
            self.fragment_id,
            Distribution::Single,
        )
        .into())
    }
}

impl ToBatchPb for BatchGetFragmentVnodes {
    fn to_batch_prost_body(&self) -> NodeBody {
        use risingwave_pb::batch_plan::GetFragmentVnodesNode;

        NodeBody::GetFragmentVnodes(GetFragmentVnodesNode {
            fragment_id: self.fragment_id,
        })
    }
}

impl ToLocalBatch for BatchGetFragmentVnodes {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(
            self.base.ctx(),
            self.base.schema().clone(),
            self.fragment_id,
            Distribution::Single,
        )
        .into())
    }
}

impl ExprRewritable<Batch> for BatchGetFragmentVnodes {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl crate::optimizer::plan_node::expr_visitable::ExprVisitable for BatchGetFragmentVnodes {
    fn visit_exprs(&self, _v: &mut dyn crate::expr::ExprVisitor) {
        // No expressions to visit
    }
}
