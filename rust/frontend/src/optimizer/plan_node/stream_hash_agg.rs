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
//
use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::expr::{column_idx_to_inputref_proto, InputRefDisplay};
use crate::optimizer::property::{Distribution, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamHashAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl StreamHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.pk_indices.to_vec();
        let input_dist = logical.input().distribution().clone();
        assert_eq!(input_dist, Distribution::Single);
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            Distribution::Single,
        );
        StreamHashAgg { logical, base }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
    pub fn group_keys(&self) -> &[usize] {
        self.logical.group_keys()
    }
}

impl fmt::Display for StreamHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamHashAgg")
            .field(
                "group_keys",
                &self
                    .group_keys()
                    .iter()
                    .copied()
                    .map(InputRefDisplay)
                    .collect_vec(),
            )
            .field("aggs", &self.agg_calls())
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamHashAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { StreamHashAgg }

impl WithSchema for StreamHashAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamHashAgg {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::HashAggNode(HashAggNode {
            group_keys: self
                .group_keys()
                .iter()
                .map(|idx| column_idx_to_inputref_proto(*idx))
                .collect_vec(),
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect_vec(),
        })
    }
}
