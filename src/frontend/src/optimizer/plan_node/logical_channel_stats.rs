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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    BatchChannelStats, ColPrunable, ExprRewritable, Logical, LogicalFilter,
    LogicalPlanRef as PlanRef, PlanBase, PredicatePushdown, ToBatch, ToStream,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalChannelStats` represents a plan that fetches channel statistics from the meta service
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalChannelStats {
    pub base: PlanBase<Logical>,
}

impl_plan_tree_node_for_leaf! { Logical, LogicalChannelStats }

impl LogicalChannelStats {
    /// Create a [`LogicalChannelStats`] node.
    pub fn new(ctx: OptimizerContextRef) -> Self {
        let fields = vec![
            Field::new("upstream_fragment_id", DataType::Int32),
            Field::new("downstream_fragment_id", DataType::Int32),
            Field::new("actor_count", DataType::Int32),
            Field::new("backpressure_rate", DataType::Float64),
            Field::new("recv_throughput", DataType::Float64),
            Field::new("send_throughput", DataType::Float64),
        ];

        let functional_dependency = FunctionalDependencySet::new(fields.len());
        let base = PlanBase::new_logical(ctx, Schema::new(fields), None, functional_dependency);
        Self { base }
    }

    /// Create a [`LogicalChannelStats`] node. Used by planner.
    pub fn create(ctx: OptimizerContextRef) -> PlanRef {
        Self::new(ctx).into()
    }
}

impl Distill for LogicalChannelStats {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("LogicalChannelStats", vec![])
    }
}

impl ExprRewritable<Logical> for LogicalChannelStats {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl ExprVisitable for LogicalChannelStats {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {
        // No expressions to visit
    }
}

impl ColPrunable for LogicalChannelStats {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // For now, we don't support column pruning on channel stats
        // as all columns are required for the complete stats view
        self.clone().into()
    }
}

impl PredicatePushdown for LogicalChannelStats {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalChannelStats {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        Ok(BatchChannelStats::new(self.clone()).into())
    }
}

impl ToStream for LogicalChannelStats {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        // For now, we don't support streaming channel stats
        // This could be implemented in the future if needed
        unimplemented!("Streaming channel stats is not supported")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        // No special rewriting needed for stream
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
