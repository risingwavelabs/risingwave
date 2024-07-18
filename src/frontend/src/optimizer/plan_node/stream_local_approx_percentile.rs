// Copyright 2024 RisingWave Labs
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
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::Distill;
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanNode, PlanTreeNodeUnary, Stream, StreamGlobalApproxPercentile,
    StreamHopWindow, StreamNode,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamLocalApproxPercentile {
    pub base: PlanBase<Stream>,
}

impl StreamLocalApproxPercentile {
    pub fn new(input: PlanRef) -> Self {
        Self { base: todo!() }
    }
}

impl Distill for StreamLocalApproxPercentile {
    fn distill<'a>(&self) -> XmlNode<'a> {
        todo!()
    }
}

impl PlanTreeNodeUnary for StreamLocalApproxPercentile {
    fn input(&self) -> PlanRef {
        todo!()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        todo!()
    }
}

impl_plan_tree_node_for_unary! {StreamLocalApproxPercentile}

impl StreamNode for StreamLocalApproxPercentile {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        todo!()
    }
}

impl ExprRewritable for StreamLocalApproxPercentile {
    fn has_rewritable_expr(&self) -> bool {
        todo!()
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        todo!()
    }
}

impl ExprVisitable for StreamLocalApproxPercentile {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        todo!()
    }
}
