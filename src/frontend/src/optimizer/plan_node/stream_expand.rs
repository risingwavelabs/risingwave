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
use risingwave_pb::stream_plan::expand_node::Subset;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::ExpandNode;

use super::{LogicalExpand, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::Distribution;

#[derive(Debug, Clone)]
pub struct StreamExpand {
    pub base: PlanBase,
    logical: LogicalExpand,
}

impl StreamExpand {
    pub fn new(logical: LogicalExpand) -> Self {
        let base = PlanBase::new_stream(
            logical.base.ctx.clone(),
            logical.schema().clone(),
            logical.base.pk_indices.to_vec(),
            Distribution::SomeShard,
            logical.input().append_only(),
        );
        StreamExpand { base, logical }
    }

    pub fn column_subsets(&self) -> &Vec<Vec<usize>> {
        self.logical.column_subsets()
    }
}

impl fmt::Display for StreamExpand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamExpand {{ column_subsets: {:?} }}",
            self.logical.column_subsets()
        )
    }
}

impl PlanTreeNodeUnary for StreamExpand {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamExpand }

impl ToStreamProst for StreamExpand {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::Expand(ExpandNode {
            column_subsets: self
                .column_subsets()
                .iter()
                .map(|subset| subset_to_protobuf(subset))
                .collect_vec(),
        })
    }
}

fn subset_to_protobuf(subset: &[usize]) -> Subset {
    let keys = subset.iter().map(|key| *key as u32).collect_vec();
    Subset { keys }
}
