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

use itertools::Itertools;
use risingwave_pb::stream_plan::ProjectSetNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode, generic};
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{
    MonotonicityMap, WatermarkColumns, analyze_monotonicity, monotonicity_variants,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamProjectSet {
    pub base: PlanBase<Stream>,
    core: generic::ProjectSet<PlanRef>,
    /// All the watermark derivations, (`input_column_idx`, `expr_idx`). And the
    /// derivation expression is the `project_set`'s expression itself.
    watermark_derivations: Vec<(usize, usize)>,
    /// Nondecreasing expression indices. `ProjectSet` can produce watermarks for these
    /// expressions.
    nondecreasing_exprs: Vec<usize>,
}

impl StreamProjectSet {
    pub fn new(core: generic::ProjectSet<PlanRef>) -> Self {
        let ctx = core.input.ctx();
        let input = core.input.clone();
        let distribution = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());

        let mut watermark_derivations = vec![];
        let mut nondecreasing_exprs = vec![];
        let mut out_watermark_columns = WatermarkColumns::new();
        for (expr_idx, expr) in core.select_list.iter().enumerate() {
            let out_expr_idx = expr_idx + 1;

            use monotonicity_variants::*;
            match analyze_monotonicity(expr) {
                Inherent(monotonicity) => {
                    if monotonicity.is_non_decreasing() && !monotonicity.is_constant() {
                        // TODO(rc): may be we should also derive watermark for constant later
                        // FIXME(rc): we need to check expr is not table function
                        // to produce watermarks
                        nondecreasing_exprs.push(expr_idx);
                        // each inherently non-decreasing expr creates a new watermark group
                        out_watermark_columns.insert(out_expr_idx, ctx.next_watermark_group_id());
                    }
                }
                FollowingInput(input_idx) => {
                    if let Some(wtmk_group) = input.watermark_columns().get_group(input_idx) {
                        // to propagate watermarks
                        watermark_derivations.push((input_idx, expr_idx));
                        // join an existing watermark group
                        out_watermark_columns.insert(out_expr_idx, wtmk_group);
                    }
                }
                _FollowingInputInversely(_) => {}
            }
        }

        // ProjectSet executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream_with_core(
            &core,
            distribution,
            input.append_only(),
            input.emit_on_window_close(),
            out_watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );
        StreamProjectSet {
            base,
            core,
            watermark_derivations,
            nondecreasing_exprs,
        }
    }
}
impl_distill_by_unit!(StreamProjectSet, core, "StreamProjectSet");
impl_plan_tree_node_for_unary! { StreamProjectSet }

impl PlanTreeNodeUnary for StreamProjectSet {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl StreamNode for StreamProjectSet {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let (watermark_input_cols, watermark_expr_indices) = self
            .watermark_derivations
            .iter()
            .map(|(i, o)| (*i as u32, *o as u32))
            .unzip();
        PbNodeBody::ProjectSet(Box::new(ProjectSetNode {
            select_list: self
                .core
                .select_list
                .iter()
                .map(|select_item| select_item.to_project_set_select_item_proto())
                .collect_vec(),
            watermark_input_cols,
            watermark_expr_indices,
            nondecreasing_exprs: self.nondecreasing_exprs.iter().map(|i| *i as _).collect(),
        }))
    }
}

impl ExprRewritable for StreamProjectSet {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for StreamProjectSet {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
