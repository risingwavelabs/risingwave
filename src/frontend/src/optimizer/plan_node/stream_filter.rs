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

use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::FilterNode;

use super::{generic, ExprRewritable, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::optimizer::plan_node::PlanBase;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::Condition;

/// `StreamFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamFilter {
    pub base: PlanBase,
    logical: generic::Filter<PlanRef>,
}

impl StreamFilter {
    pub fn new(logical: generic::Filter<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&logical);
        let ctx = base.ctx;
        let input = logical.input.clone();
        let pk_indices = base.logical_pk;
        let dist = input.distribution().clone();
        // Filter executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            base.schema,
            pk_indices,
            base.functional_dependency,
            dist,
            input.append_only(),
            input.watermark_columns().clone(),
        );
        StreamFilter { base, logical }
    }

    pub fn predicate(&self) -> &Condition {
        &self.logical.predicate
    }
}

impl fmt::Display for StreamFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamFilter")
    }
}

impl PlanTreeNodeUnary for StreamFilter {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { StreamFilter }

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
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
