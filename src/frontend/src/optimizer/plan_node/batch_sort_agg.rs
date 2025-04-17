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

use risingwave_pb::batch_plan::SortAggNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::expr::ExprNode;

use super::batch::prelude::*;
use super::generic::{self, PlanAggCall};
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch};
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::{ColIndexMappingRewriteExt, IndexSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSortAgg {
    pub base: PlanBase<Batch>,
    core: generic::Agg<PlanRef>,
    input_order: Order,
}

impl BatchSortAgg {
    pub fn new(core: generic::Agg<PlanRef>) -> Self {
        assert!(!core.group_key.is_empty());
        assert!(core.input_provides_order_on_group_keys());

        let input = core.input.clone();
        let input_dist = input.distribution();
        let dist = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(input_dist);
        let input_order = Order {
            column_orders: input
                .order()
                .column_orders
                .iter()
                .filter(|o| core.group_key.indices().any(|g_k| g_k == o.column_index))
                .cloned()
                .collect(),
        };

        let order = core.i2o_col_mapping().rewrite_provided_order(&input_order);

        let base = PlanBase::new_batch_with_core(&core, dist, order);

        BatchSortAgg {
            base,
            core,
            input_order,
        }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }

    pub fn group_key(&self) -> &IndexSet {
        &self.core.group_key
    }
}

impl PlanTreeNodeUnary for BatchSortAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}
impl_plan_tree_node_for_unary! { BatchSortAgg }
impl_distill_by_unit!(BatchSortAgg, core, "BatchSortAgg");

impl ToDistributedBatch for BatchSortAgg {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed_with_required(
            &self.input_order,
            &RequiredDist::shard_by_key(self.input().schema().len(), &self.group_key().to_vec()),
        )?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchSortAgg {
    fn to_batch_prost_body(&self) -> NodeBody {
        let input = self.input();
        NodeBody::SortAgg(SortAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            group_key: self
                .group_key()
                .indices()
                .map(|idx| {
                    ExprImpl::InputRef(InputRef::new(idx, input.schema()[idx].data_type()).into())
                })
                .map(|expr| expr.to_expr_proto())
                .collect::<Vec<ExprNode>>(),
        })
    }
}

impl ToLocalBatch for BatchSortAgg {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;

        let new_input =
            RequiredDist::single().enforce_if_not_satisfies(new_input, self.input().order())?;

        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchSortAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new_logical = self.core.clone();
        new_logical.rewrite_exprs(r);
        Self::new(new_logical).into()
    }
}

impl ExprVisitable for BatchSortAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
