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

use itertools::Itertools;
use risingwave_common::catalog::FieldDisplay;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::HopWindowNode;

use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// [`StreamHopWindow`] represents a hop window table function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHopWindow {
    pub base: PlanBase,
    logical: generic::HopWindow<PlanRef>,
    window_start_exprs: Vec<ExprImpl>,
    window_end_exprs: Vec<ExprImpl>,
}

impl StreamHopWindow {
    pub fn new(
        logical: generic::HopWindow<PlanRef>,
        window_start_exprs: Vec<ExprImpl>,
        window_end_exprs: Vec<ExprImpl>,
    ) -> Self {
        let base = PlanBase::new_logical_with_core(&logical);
        let ctx = base.ctx;
        let pk_indices = base.logical_pk;
        let input = logical.input.clone();
        let schema = base.schema;

        let i2o = logical.i2o_col_mapping();
        let dist = i2o.rewrite_provided_distribution(input.distribution());

        let mut watermark_columns = input.watermark_columns().clone();
        watermark_columns.grow(logical.internal_column_num());

        if watermark_columns.contains(logical.time_col.index) {
            // Watermark on `time_col` indicates watermark on both `window_start` and `window_end`.
            watermark_columns.insert(logical.internal_window_start_col_idx());
            watermark_columns.insert(logical.internal_window_end_col_idx());
        }
        let watermark_columns = ColIndexMapping::with_remaining_columns(
            &logical.output_indices,
            logical.internal_column_num(),
        )
        .rewrite_bitset(&watermark_columns);

        let base = PlanBase::new_stream(
            ctx,
            schema,
            pk_indices,
            base.functional_dependency,
            dist,
            logical.input.append_only(),
            watermark_columns,
        );
        Self {
            base,
            logical,
            window_start_exprs,
            window_end_exprs,
        }
    }
}

impl fmt::Display for StreamHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamHopWindow");
        self.logical.fmt_fields_with_builder(&mut builder);

        let watermark_columns = &self.base.watermark_columns;
        if self.base.watermark_columns.count_ones(..) > 0 {
            let schema = self.schema();
            builder.field(
                "output_watermarks",
                &watermark_columns
                    .ones()
                    .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
                    .collect_vec(),
            );
        };

        builder.finish()
    }
}

impl PlanTreeNodeUnary for StreamHopWindow {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(
            logical,
            self.window_start_exprs.clone(),
            self.window_end_exprs.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! {StreamHopWindow}

impl StreamNode for StreamHopWindow {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::HopWindow(HopWindowNode {
            time_col: self.logical.time_col.index() as _,
            window_slide: Some(self.logical.window_slide.into()),
            window_size: Some(self.logical.window_size.into()),
            output_indices: self
                .logical
                .output_indices
                .iter()
                .map(|&x| x as u32)
                .collect(),
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
            self.logical.clone(),
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
