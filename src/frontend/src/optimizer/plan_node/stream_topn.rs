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

use super::{LogicalTopN, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, FieldOrder};

/// `StreamTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone)]
pub struct StreamTopN {
    pub base: PlanBase,
    logical: LogicalTopN,
}

impl StreamTopN {
    pub fn new(logical: LogicalTopN) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = match logical.input().distribution() {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.input().pk_indices().to_vec(),
            dist,
            false,
        );
        StreamTopN { base, logical }
    }
}

impl fmt::Display for StreamTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = if self.input().append_only() {
            f.debug_struct("StreamAppendOnlyTopN")
        } else {
            f.debug_struct("StreamTopN")
        };

        builder
            .field("order", &format_args!("{}", self.logical.topn_order()))
            .field("limit", &format_args!("{}", self.logical.limit()))
            .field("offset", &format_args!("{}", self.logical.offset()))
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamTopN {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamTopN }

impl ToStreamProst for StreamTopN {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let column_orders = self
            .logical
            .topn_order()
            .field_order
            .iter()
            .map(FieldOrder::to_protobuf)
            .collect();

        let topn_node = TopNNode {
            column_orders,
            limit: self.logical.limit() as u64,
            offset: self.logical.offset() as u64,
            distribution_key: vec![], // TODO: seems unnecessary
            ..Default::default()
        };

        if self.input().append_only() {
            ProstStreamNode::AppendOnlyTopN(topn_node)
        } else {
            ProstStreamNode::TopN(topn_node)
        }
    }
}
