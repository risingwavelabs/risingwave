// Copyright 2025 RisingWave Labs
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

use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::stream::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef,
    StreamPlanRef,
};
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{MonotonicityMap, RequiredDist, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// Streaming stateless simple agg.
///
/// Should only be used for stateless agg, including `sum`, `count` and *append-only* `min`/`max`.
///
/// The output of `StreamStatelessSimpleAgg` doesn't have pk columns, so the result can only be used
/// by `StreamSimpleAgg`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamStatelessSimpleAgg {
    pub base: PlanBase<Stream>,
    core: generic::Agg<PlanRef>,
}

impl StreamStatelessSimpleAgg {
    pub fn new(core: generic::Agg<StreamPlanRef>) -> Self {
        let input = core.input.clone();
        let input_dist = input.distribution();
        debug_assert!(input_dist.satisfies(&RequiredDist::AnyShard));

        let mut watermark_columns = WatermarkColumns::new();
        // Watermark column(s) must be in group key.
        for (idx, input_idx) in core.group_key.indices().enumerate() {
            if let Some(wtmk_group) = input.watermark_columns().get_group(input_idx) {
                watermark_columns.insert(idx, wtmk_group);
            }
        }

        let base = PlanBase::new_stream_with_core(
            &core,
            input_dist.clone(),
            // Stateless simple agg outputs one `Insert` row per epoch to the global phase.
            // TODO(kind): reject upsert input
            StreamKind::AppendOnly,
            input.emit_on_window_close(),
            watermark_columns,
            MonotonicityMap::new(),
        );
        StreamStatelessSimpleAgg { base, core }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }
}
impl_distill_by_unit!(StreamStatelessSimpleAgg, core, "StreamStatelessSimpleAgg");

impl PlanTreeNodeUnary<Stream> for StreamStatelessSimpleAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}
impl_plan_tree_node_for_unary! { Stream, StreamStatelessSimpleAgg }

impl StreamNode for StreamStatelessSimpleAgg {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        PbNodeBody::StatelessSimpleAgg(Box::new(SimpleAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            row_count_index: u32::MAX, // this is not used
            agg_call_states: vec![],
            intermediate_state_table: None,
            is_append_only: self.input().append_only(),
            distinct_dedup_tables: Default::default(),
            version: AggNodeVersion::Issue13465 as _,
            must_output_per_barrier: false, // this is not used
        }))
    }
}

impl ExprRewritable<Stream> for StreamStatelessSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for StreamStatelessSimpleAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
