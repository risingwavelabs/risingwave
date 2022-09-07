// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::ProjectNode;

use super::{LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::Expr;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone)]
pub struct StreamProject {
    pub base: PlanBase,
    logical: LogicalProject,
}

impl fmt::Display for StreamProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamProject")
    }
}

impl StreamProject {
    pub fn new(logical: LogicalProject) -> Self {
        let ctx = logical.base.ctx.clone();
        let input = logical.input();
        let pk_indices = logical.base.logical_pk.to_vec();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());
        // Project executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.functional_dependency().clone(),
            distribution,
            logical.input().append_only(),
        );
        StreamProject { base, logical }
    }

    pub fn as_logical(&self) -> &LogicalProject {
        &self.logical
    }
}

impl PlanTreeNodeUnary for StreamProject {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! {StreamProject}

impl StreamNode for StreamProject {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::Project(ProjectNode {
            select_list: self
                .logical
                .exprs()
                .iter()
                .map(Expr::to_expr_proto)
                .collect(),
        })
    }
}
