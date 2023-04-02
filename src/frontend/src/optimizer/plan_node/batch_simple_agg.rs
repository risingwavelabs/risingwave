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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::SortAggNode;

use super::generic::{self, PlanAggCall};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch};
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::{BatchExchange, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSimpleAgg {
    pub base: PlanBase,
    logical: generic::Agg<PlanRef>,
}

impl BatchSimpleAgg {
    pub fn new(logical: generic::Agg<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&logical);
        let ctx = base.ctx;
        let input = logical.input.clone();
        let input_dist = input.distribution();
        let base = PlanBase::new_batch(ctx, base.schema, input_dist.clone(), Order::any());
        BatchSimpleAgg { base, logical }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.logical.agg_calls
    }

    fn two_phase_agg_enabled(&self) -> bool {
        let session_ctx = self.base.ctx.session_ctx();
        session_ctx.config().get_enable_two_phase_agg()
    }

    pub(crate) fn can_two_phase_agg(&self) -> bool {
        self.logical.can_two_phase_agg() && self.two_phase_agg_enabled()
    }
}

impl fmt::Display for BatchSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchSimpleAgg")
    }
}

impl PlanTreeNodeUnary for BatchSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(generic::Agg {
            input,
            ..self.logical.clone()
        })
    }
}
impl_plan_tree_node_for_unary! { BatchSimpleAgg }

impl ToDistributedBatch for BatchSimpleAgg {
    fn to_distributed(&self) -> Result<PlanRef> {
        // Ensure input is distributed, batch phase might not distribute it
        // (e.g. see distribution of BatchSeqScan::new vs BatchSeqScan::to_distributed)
        let dist_input = self.input().to_distributed()?;

        // TODO: distinct agg cannot use 2-phase agg yet.
        if dist_input.distribution().satisfies(&RequiredDist::AnyShard) && self.can_two_phase_agg()
        {
            // partial agg
            let partial_agg = self.clone_with_input(dist_input).into();

            // insert exchange
            let exchange =
                BatchExchange::new(partial_agg, Order::any(), Distribution::Single).into();

            // insert total agg
            let total_agg_types = self
                .logical
                .agg_calls
                .iter()
                .enumerate()
                .map(|(partial_output_idx, agg_call)| {
                    agg_call.partial_to_total_agg_call(partial_output_idx)
                })
                .collect();
            let total_agg_logical =
                generic::Agg::new(total_agg_types, self.logical.group_key.to_vec(), exchange);
            Ok(BatchSimpleAgg::new(total_agg_logical).into())
        } else {
            let new_input = self
                .input()
                .to_distributed_with_required(&Order::any(), &RequiredDist::single())?;
            Ok(self.clone_with_input(new_input).into())
        }
    }
}

impl ToBatchPb for BatchSimpleAgg {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::SortAgg(SortAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            // We treat simple agg as a special sort agg without group key.
            group_key: vec![],
        })
    }
}

impl ToLocalBatch for BatchSimpleAgg {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;

        let new_input =
            RequiredDist::single().enforce_if_not_satisfies(new_input, &Order::any())?;

        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
