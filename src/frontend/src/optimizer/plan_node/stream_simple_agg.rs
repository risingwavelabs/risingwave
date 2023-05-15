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
use itertools::Itertools;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::ExprRewriter;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSimpleAgg {
    pub base: PlanBase,
    logical: generic::Agg<PlanRef>,

    /// The index of `count(*)` in `agg_calls`.
    row_count_idx: usize,
}

impl StreamSimpleAgg {
    pub fn new(logical: generic::Agg<PlanRef>, row_count_idx: usize) -> Self {
        assert_eq!(logical.agg_calls[row_count_idx], PlanAggCall::count_star());

        let input = logical.input.clone();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        // Empty because watermark column(s) must be in group key and simple agg have no group key.
        let watermark_columns = FixedBitSet::with_capacity(logical.output_len());

        // Simple agg executor might change the append-only behavior of the stream.
        let base =
            PlanBase::new_stream_with_logical(&logical, dist, false, false, watermark_columns);
        StreamSimpleAgg {
            base,
            logical,
            row_count_idx,
        }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.logical.agg_calls
    }
}

impl fmt::Display for StreamSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(
            f,
            if self.input().append_only() {
                "StreamAppendOnlySimpleAgg"
            } else {
                "StreamSimpleAgg"
            },
        )
    }
}

impl PlanTreeNodeUnary for StreamSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let logical = generic::Agg {
            input,
            ..self.logical.clone()
        };
        Self::new(logical, self.row_count_idx)
    }
}
impl_plan_tree_node_for_unary! { StreamSimpleAgg }

impl StreamNode for StreamSimpleAgg {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        let (result_table, agg_states, distinct_dedup_tables) =
            self.logical.infer_tables(&self.base, None);

        PbNodeBody::SimpleAgg(SimpleAggNode {
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
            is_append_only: self.input().append_only(),
            agg_call_states: agg_states
                .into_iter()
                .map(|s| s.into_prost(state))
                .collect(),
            result_table: Some(
                result_table
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            distinct_dedup_tables: distinct_dedup_tables
                .into_iter()
                .sorted_by_key(|(i, _)| *i)
                .map(|(key_idx, table)| {
                    (
                        key_idx as u32,
                        table
                            .with_id(state.gen_table_id_wrapped())
                            .to_internal_table_prost(),
                    )
                })
                .collect(),
            row_count_index: self.row_count_idx as u32,
        })
    }
}

impl ExprRewritable for StreamSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical, self.row_count_idx).into()
    }
}
