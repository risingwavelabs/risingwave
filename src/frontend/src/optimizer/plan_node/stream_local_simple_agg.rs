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
use crate::optimizer::property::RequiredDist;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// Streaming local simple agg.
///
/// Should only be used for stateless agg, including `sum`, `count` and *append-only* `min`/`max`.
///
/// The output of `StreamLocalSimpleAgg` doesn't have pk columns, so the result can only
/// be used by `StreamGlobalSimpleAgg` with `ManagedValueState`s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamLocalSimpleAgg {
    pub base: PlanBase,
    logical: generic::Agg<PlanRef>,
}

impl StreamLocalSimpleAgg {
    pub fn new(logical: generic::Agg<PlanRef>) -> Self {
        let input = logical.input.clone();
        let input_dist = input.distribution();
        debug_assert!(input_dist.satisfies(&RequiredDist::AnyShard));

        let mut watermark_columns = FixedBitSet::with_capacity(logical.output_len());
        // Watermark column(s) must be in group key.
        for (idx, &input_idx) in logical.group_key.ones().enumerate() {
            if input.watermark_columns().contains(input_idx) {
                watermark_columns.insert(idx);
            }
        }

        let base = PlanBase::new_stream_with_logical(
            &logical,
            input_dist.clone(),
            input.append_only(),
            watermark_columns,
        );
        StreamLocalSimpleAgg { base, logical }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.logical.agg_calls
    }
}

impl fmt::Display for StreamLocalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical
            .fmt_with_name(f, "StreamStatelessLocalSimpleAgg")
    }
}

impl PlanTreeNodeUnary for StreamLocalSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}
impl_plan_tree_node_for_unary! { StreamLocalSimpleAgg }

impl StreamNode for StreamLocalSimpleAgg {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        PbNodeBody::LocalSimpleAgg(SimpleAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            row_count_index: u32::MAX, // this is not used
            distribution_key: self
                .distribution()
                .dist_column_indices()
                .iter()
                .map(|idx| *idx as u32)
                .collect_vec(),
            agg_call_states: vec![],
            result_table: None,
            is_append_only: self.input().append_only(),
            distinct_dedup_tables: Default::default(),
        })
    }
}

impl ExprRewritable for StreamLocalSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
