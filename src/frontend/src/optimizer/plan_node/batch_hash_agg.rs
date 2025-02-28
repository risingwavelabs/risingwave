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
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::generic::{self, PlanAggCall};
use super::utils::impl_distill_by_unit;
use super::{
    ExprRewritable, PlanBase, PlanNodeType, PlanRef, PlanTreeNodeUnary, ToBatchPb,
    ToDistributedBatch,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::{ColIndexMappingRewriteExt, IndexSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchHashAgg {
    pub base: PlanBase<Batch>,
    core: generic::Agg<PlanRef>,
}

impl BatchHashAgg {
    pub fn new(core: generic::Agg<PlanRef>) -> Self {
        assert!(!core.group_key.is_empty());
        let input = core.input.clone();
        let input_dist = input.distribution();
        let dist = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(input_dist);
        let base = PlanBase::new_batch_with_core(&core, dist, Order::any());
        BatchHashAgg { base, core }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }

    pub fn group_key(&self) -> &IndexSet {
        &self.core.group_key
    }

    fn to_two_phase_agg(&self, dist_input: PlanRef) -> Result<PlanRef> {
        // partial agg - follows input distribution
        let partial_agg: PlanRef = self.clone_with_input(dist_input).into();
        debug_assert!(partial_agg.node_type() == PlanNodeType::BatchHashAgg);

        // insert exchange
        let exchange = RequiredDist::shard_by_key(
            partial_agg.schema().len(),
            &(0..self.group_key().len()).collect_vec(),
        )
        .enforce_if_not_satisfies(partial_agg, &Order::any())?;

        // insert total agg
        let total_agg_types = self
            .core
            .agg_calls
            .iter()
            .enumerate()
            .map(|(partial_output_idx, agg_call)| {
                agg_call.partial_to_total_agg_call(partial_output_idx + self.group_key().len())
            })
            .collect();
        let total_agg_logical = generic::Agg::new(
            total_agg_types,
            (0..self.group_key().len()).collect(),
            exchange,
        );
        Ok(BatchHashAgg::new(total_agg_logical).into())
    }

    fn to_shuffle_agg(&self) -> Result<PlanRef> {
        let input = self.input();
        let required_dist = RequiredDist::shard_by_key(
            input.schema().len(),
            &self.group_key().indices().collect_vec(),
        );
        let new_input = input.to_distributed_with_required(&Order::any(), &required_dist)?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl_distill_by_unit!(BatchHashAgg, core, "BatchHashAgg");

impl PlanTreeNodeUnary for BatchHashAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { BatchHashAgg }
impl ToDistributedBatch for BatchHashAgg {
    fn to_distributed(&self) -> Result<PlanRef> {
        if self.core.must_try_two_phase_agg() {
            let input = self.input().to_distributed()?;
            let input_dist = input.distribution();
            if !self.core.hash_agg_dist_satisfied_by_input_dist(input_dist)
                && matches!(
                    input_dist,
                    Distribution::HashShard(_)
                        | Distribution::UpstreamHashShard(_, _)
                        | Distribution::SomeShard
                )
            {
                return self.to_two_phase_agg(input);
            }
        }
        self.to_shuffle_agg()
    }
}

impl ToBatchPb for BatchHashAgg {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HashAgg(HashAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            group_key: self.group_key().to_vec_as_u32(),
        })
    }
}

impl ToLocalBatch for BatchHashAgg {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;

        let new_input =
            RequiredDist::single().enforce_if_not_satisfies(new_input, &Order::any())?;

        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchHashAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for BatchHashAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
