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

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::plan_node::PlanAggCallDisplay;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone)]
pub struct StreamGlobalSimpleAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl StreamGlobalSimpleAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.logical_pk.to_vec();
        let input = logical.input();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        // Simple agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.functional_dependency().clone(),
            dist,
            false,
        );
        StreamGlobalSimpleAgg { base, logical }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }

    pub fn agg_calls_verbose_display(&self) -> Vec<PlanAggCallDisplay> {
        self.logical.agg_calls_display()
    }
}

impl fmt::Display for StreamGlobalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.input().append_only() {
            self.logical
                .fmt_with_name(f, "StreamAppendOnlyGlobalSimpleAgg")
        } else {
            self.logical.fmt_with_name(f, "StreamGlobalSimpleAgg")
        }
    }
}

impl PlanTreeNodeUnary for StreamGlobalSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { StreamGlobalSimpleAgg }

impl ToStreamProst for StreamGlobalSimpleAgg {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let (internal_tables, column_mappings) = self.logical.infer_internal_table_catalog(None);
        ProstStreamNode::GlobalSimpleAgg(SimpleAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            distribution_key: self
                .base
                .dist
                .dist_column_indices()
                .iter()
                .map(|idx| *idx as u32)
                .collect(),
            internal_tables: internal_tables
                .into_iter()
                .map(|table| {
                    table
                        .with_id(state.gen_table_id_wrapped())
                        .to_state_table_prost()
                })
                .collect(),
            column_mappings: column_mappings
                .into_iter()
                .map(|v| ColumnMapping {
                    indices: v.iter().map(|x| *x as u32).collect(),
                })
                .collect(),
            is_append_only: self.input().append_only(),
        })
    }
}
