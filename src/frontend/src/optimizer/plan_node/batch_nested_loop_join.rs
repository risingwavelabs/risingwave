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

use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::NestedLoopJoinNode;

use super::generic::{self, GenericPlanRef};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatchPb, ToDistributedBatch};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::ConditionDisplay;

/// `BatchNestedLoopJoin` implements [`super::LogicalJoin`] by checking the join condition
/// against all pairs of rows from inner & outer side within 2 layers of loops.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchNestedLoopJoin {
    pub base: PlanBase,
    logical: generic::Join<PlanRef>,
}

impl BatchNestedLoopJoin {
    pub fn new(logical: generic::Join<PlanRef>) -> Self {
        let dist = Self::derive_dist(logical.left.distribution(), logical.right.distribution());
        let base = PlanBase::new_batch_from_logical(&logical, dist, Order::any());
        Self { base, logical }
    }

    fn derive_dist(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (_, _) => unreachable!("{}{}", left, right),
        }
    }
}

impl fmt::Display for BatchNestedLoopJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("BatchNestedLoopJoin");
        builder.field("type", &self.logical.join_type);

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "predicate",
            &ConditionDisplay {
                condition: &self.logical.on,
                input_schema: &concat_schema,
            },
        );

        if verbose {
            if self
                .logical
                .output_indices
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: &self.logical.output_indices,
                        input_schema: &concat_schema,
                    },
                );
            }
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for BatchNestedLoopJoin {
    fn left(&self) -> PlanRef {
        self.logical.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.logical.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.left = left;
        logical.right = right;
        Self::new(logical)
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
            join_type: self.logical.join_type as i32,
            join_cond: Some(ExprImpl::from(self.logical.on.clone()).to_expr_proto()),
            output_indices: self
                .logical
                .output_indices
                .iter()
                .map(|&x| x as u32)
                .collect(),
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
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
