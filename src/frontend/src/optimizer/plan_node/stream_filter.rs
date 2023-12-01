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

use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::FilterNode;

use super::stream::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{generic, ExprRewritable, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::PlanBase;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::Condition;

/// `StreamFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFilter {
    pub base: PlanBase<Stream>,
    core: generic::Filter<PlanRef>,
}

impl StreamFilter {
    pub fn new(core: generic::Filter<PlanRef>) -> Self {
        let input = core.input.clone();
        let dist = input.distribution().clone();
        // Filter executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        StreamFilter { base, core }
    }

    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }
}

impl PlanTreeNodeUnary for StreamFilter {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { StreamFilter }
impl_distill_by_unit!(StreamFilter, core, "StreamFilter");

impl StreamNode for StreamFilter {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::Filter(FilterNode {
            search_condition: Some(ExprImpl::from(self.predicate().clone()).to_expr_proto()),
        })
    }
}

impl ExprRewritable for StreamFilter {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for StreamFilter {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}
