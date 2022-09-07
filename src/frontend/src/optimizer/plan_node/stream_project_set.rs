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

use itertools::Itertools;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::ProjectSetNode;

use super::{LogicalProjectSet, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone)]
pub struct StreamProjectSet {
    pub base: PlanBase,
    logical: LogicalProjectSet,
}

impl StreamProjectSet {
    pub fn new(logical: LogicalProjectSet) -> Self {
        let ctx = logical.base.ctx.clone();
        let input = logical.input();
        let pk_indices = logical.base.logical_pk.to_vec();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());
        // ProjectSet executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.functional_dependency().clone(),
            distribution,
            logical.input().append_only(),
        );
        StreamProjectSet { base, logical }
    }
}

impl fmt::Display for StreamProjectSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamProjectSet")
    }
}

impl PlanTreeNodeUnary for StreamProjectSet {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamProjectSet }

impl StreamNode for StreamProjectSet {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::ProjectSet(ProjectSetNode {
            select_list: self
                .logical
                .select_list()
                .iter()
                .map(|select_item| select_item.to_project_set_select_item_proto())
                .collect_vec(),
        })
    }
}
