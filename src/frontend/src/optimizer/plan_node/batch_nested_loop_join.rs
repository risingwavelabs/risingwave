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
use risingwave_pb::batch_plan::NestedLoopJoinNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatchPb, ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::ConditionDisplay;

/// `BatchNestedLoopJoin` implements [`super::LogicalJoin`] by checking the join condition
/// against all pairs of rows from inner & outer side within 2 layers of loops.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchNestedLoopJoin {
    pub base: PlanBase<Batch>,
    core: generic::Join<PlanRef>,
}

impl BatchNestedLoopJoin {
    pub fn new(core: generic::Join<PlanRef>) -> Self {
        let dist = Self::derive_dist(core.left.distribution(), core.right.distribution());
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        Self { base, core }
    }

    fn derive_dist(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (_, _) => unreachable!("{}{}", left, right),
        }
    }
}

impl Distill for BatchNestedLoopJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.core.join_type)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&ConditionDisplay {
                condition: &self.core.on,
                input_schema: &concat_schema,
            }),
        ));

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        childless_record("BatchNestedLoopJoin", vec)
    }
}

impl PlanTreeNodeBinary for BatchNestedLoopJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = left;
        core.right = right;
        Self::new(core)
    }
}

impl_plan_tree_node_for_binary! { BatchNestedLoopJoin }

impl ToDistributedBatch for BatchNestedLoopJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let left = self
            .left()
            .to_distributed_with_required(&Order::any(), &RequiredDist::single())?;
        let right = self
            .right()
            .to_distributed_with_required(&Order::any(), &RequiredDist::single())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}

impl ToBatchPb for BatchNestedLoopJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::NestedLoopJoin(NestedLoopJoinNode {
            join_type: self.core.join_type as i32,
            join_cond: Some(ExprImpl::from(self.core.on.clone()).to_expr_proto()),
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
        })
    }
}

impl ToLocalBatch for BatchNestedLoopJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let left = RequiredDist::single()
            .enforce_if_not_satisfies(self.left().to_local()?, &Order::any())?;

        let right = RequiredDist::single()
            .enforce_if_not_satisfies(self.right().to_local()?, &Order::any())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}

impl ExprRewritable for BatchNestedLoopJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for BatchNestedLoopJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
