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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::ChannelStatsNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, LogicalChannelStats, PlanBase, PlanTreeNodeLeaf,
    ToBatchPb, ToDistributedBatch,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchChannelStats {
    pub base: PlanBase<Batch>,
    logical: LogicalChannelStats,
}

impl PlanTreeNodeLeaf for BatchChannelStats {}
impl_plan_tree_node_for_leaf! { Batch, BatchChannelStats }

impl BatchChannelStats {
    pub fn new(logical: LogicalChannelStats) -> Self {
        Self::with_dist(logical, Distribution::Single)
    }

    pub fn with_dist(logical: LogicalChannelStats, dist: Distribution) -> Self {
        let ctx = logical.base.ctx().clone();
        let base = PlanBase::new_batch(ctx, logical.base.schema().clone(), dist, Order::any());
        BatchChannelStats { base, logical }
    }

    /// Get a reference to the batch channel stats's logical.
    #[must_use]
    pub fn logical(&self) -> &LogicalChannelStats {
        &self.logical
    }
}

impl Distill for BatchChannelStats {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("BatchChannelStats", vec![])
    }
}

impl ToDistributedBatch for BatchChannelStats {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}

impl ToBatchPb for BatchChannelStats {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::ChannelStats(ChannelStatsNode {
            fields: self
                .logical
                .base
                .schema()
                .fields()
                .iter()
                .map(|f| f.to_prost())
                .collect(),
        })
    }
}

impl ToLocalBatch for BatchChannelStats {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}

impl ExprRewritable<Batch> for BatchChannelStats {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl ExprVisitable for BatchChannelStats {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {
        // No expressions to visit
    }
}
