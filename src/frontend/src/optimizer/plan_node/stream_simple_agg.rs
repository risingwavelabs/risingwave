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
use risingwave_common::catalog::{DatabaseId, SchemaId};
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::Distribution;

#[derive(Debug, Clone)]
pub struct StreamSimpleAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl StreamSimpleAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.pk_indices.to_vec();
        let input = logical.input();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        // Simple agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream(ctx, logical.schema().clone(), pk_indices, dist, false);
        StreamSimpleAgg { base, logical }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
}

impl fmt::Display for StreamSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = if self.input().append_only() {
            f.debug_struct("StreamAppendOnlySimpleAgg")
        } else {
            f.debug_struct("StreamSimpleAgg")
        };
        builder.field("aggs", &self.agg_calls());
        builder.finish()
    }
}

impl PlanTreeNodeUnary for StreamSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { StreamSimpleAgg }

impl ToStreamProst for StreamSimpleAgg {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let (internal_tables, column_mapping) = self.logical.infer_internal_table_catalog();
        // TODO: local or global simple agg?
        ProstStreamNode::GlobalSimpleAgg(SimpleAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            distribution_keys: self
                .base
                .dist
                .dist_column_indices()
                .iter()
                .map(|idx| *idx as u32)
                .collect_vec(),
            internal_tables: internal_tables
                .into_iter()
                .map(|table_catalog| {
                    table_catalog.to_prost(
                        SchemaId::placeholder() as u32,
                        DatabaseId::placeholder() as u32,
                    )
                })
                .collect_vec(),
            column_mapping: column_mapping
                .into_iter()
                .map(|(k, v)| (k as u32, v))
                .collect(),
            is_append_only: self.input().append_only(),
        })
    }
}
