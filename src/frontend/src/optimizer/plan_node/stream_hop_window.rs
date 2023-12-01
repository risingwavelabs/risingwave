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

use pretty_xmlish::XmlNode;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::HopWindowNode;

use super::generic::GenericPlanRef;
use super::stream::prelude::*;
use super::stream::StreamPlanRef;
use super::utils::{childless_record, watermark_pretty, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// [`StreamHopWindow`] represents a hop window table function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHopWindow {
    pub base: PlanBase<Stream>,
    core: generic::HopWindow<PlanRef>,
    window_start_exprs: Vec<ExprImpl>,
    window_end_exprs: Vec<ExprImpl>,
}

impl StreamHopWindow {
    pub fn new(
        core: generic::HopWindow<PlanRef>,
        window_start_exprs: Vec<ExprImpl>,
        window_end_exprs: Vec<ExprImpl>,
    ) -> Self {
        let input = core.input.clone();
        let i2o = core.i2o_col_mapping();
        let dist = i2o.rewrite_provided_distribution(input.distribution());

        let mut watermark_columns = input.watermark_columns().clone();
        watermark_columns.grow(core.internal_column_num());

        if watermark_columns.contains(core.time_col.index) {
            // Watermark on `time_col` indicates watermark on both `window_start` and `window_end`.
            watermark_columns.insert(core.internal_window_start_col_idx());
            watermark_columns.insert(core.internal_window_end_col_idx());
        }
        let watermark_columns = ColIndexMapping::with_remaining_columns(
            &core.output_indices,
            core.internal_column_num(),
        )
        .rewrite_bitset(&watermark_columns);

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            input.append_only(),
            input.emit_on_window_close(),
            watermark_columns,
        );
        Self {
            base,
            core,
            window_start_exprs,
            window_end_exprs,
        }
    }
}

impl Distill for StreamHopWindow {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut vec = self.core.fields_pretty();
        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }
        childless_record("StreamHopWindow", vec)
    }
}

impl PlanTreeNodeUnary for StreamHopWindow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(
            core,
            self.window_start_exprs.clone(),
            self.window_end_exprs.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! {StreamHopWindow}

impl StreamNode for StreamHopWindow {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::HopWindow(HopWindowNode {
            time_col: self.core.time_col.index() as _,
            window_slide: Some(self.core.window_slide.into()),
            window_size: Some(self.core.window_size.into()),
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            window_start_exprs: self
                .window_start_exprs
                .clone()
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
            window_end_exprs: self
                .window_end_exprs
                .clone()
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
        })
    }
}

impl ExprRewritable for StreamHopWindow {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.core.clone(),
            self.window_start_exprs
                .clone()
                .into_iter()
                .map(|e| r.rewrite_expr(e))
                .collect(),
            self.window_end_exprs
                .clone()
                .into_iter()
                .map(|e| r.rewrite_expr(e))
                .collect(),
        )
        .into()
    }
}

impl ExprVisitable for StreamHopWindow {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.window_start_exprs.iter().for_each(|e| v.visit_expr(e));
        self.window_end_exprs.iter().for_each(|e| v.visit_expr(e));
    }
}
