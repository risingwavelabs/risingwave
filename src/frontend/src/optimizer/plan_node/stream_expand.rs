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

use fixedbitset::FixedBitSet;
use risingwave_pb::stream_plan::expand_node::Subset;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::ExpandNode;

use super::{ExprRewritable, LogicalExpand, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamExpand {
    pub base: PlanBase,
    logical: LogicalExpand,
}

impl StreamExpand {
    pub fn new(logical: LogicalExpand) -> Self {
        let input = logical.input();
        let schema = logical.schema().clone();

        let dist = match input.distribution() {
            Distribution::Single => Distribution::Single,
            Distribution::SomeShard
            | Distribution::HashShard(_)
            | Distribution::UpstreamHashShard(_, _) => Distribution::SomeShard,
            Distribution::Broadcast => unreachable!(),
        };

        let mut watermark_columns = FixedBitSet::with_capacity(schema.len());
        watermark_columns.extend(
            input
                .watermark_columns()
                .ones()
                .map(|idx| idx + input.schema().len()),
        );

        let base = PlanBase::new_stream(
            logical.base.ctx.clone(),
            schema,
            logical.base.logical_pk.to_vec(),
            logical.functional_dependency().clone(),
            dist,
            input.append_only(),
            watermark_columns,
        );
        StreamExpand { base, logical }
    }

    pub fn column_subsets(&self) -> &Vec<Vec<usize>> {
        self.logical.column_subsets()
    }
}

impl fmt::Display for StreamExpand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamExpand")
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

impl StreamNode for StreamExpand {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::Expand(ExpandNode {
            column_subsets: self
                .column_subsets()
                .iter()
                .map(|subset| subset_to_protobuf(subset))
                .collect(),
        })
    }
}

fn subset_to_protobuf(subset: &[usize]) -> Subset {
    let column_indices = subset.iter().map(|key| *key as u32).collect();
    Subset { column_indices }
}

impl ExprRewritable for StreamExpand {}
