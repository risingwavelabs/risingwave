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

use fixedbitset::FixedBitSet;
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::ProjectNode;

use super::stream::StreamPlanRef;
use super::utils::{childless_record, watermark_pretty, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{try_derive_watermark, Expr, ExprImpl, ExprRewriter, WatermarkDerivation};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// `StreamProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamProject {
    pub base: PlanBase,
    logical: generic::Project<PlanRef>,
    /// All the watermark derivations, (input_column_index, output_column_index). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: Vec<(usize, usize)>,
    /// Nondecreasing expression indices. `Project` can produce watermarks for these expressions.
    nondecreasing_exprs: Vec<usize>,
}

impl Distill for StreamProject {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let schema = self.schema();
        let mut vec = self.logical.fields_pretty(schema);
        if let Some(display_output_watermarks) =
            watermark_pretty(&self.base.watermark_columns, schema)
        {
            vec.push(("output_watermarks", display_output_watermarks));
        }
        childless_record("StreamProject", vec)
    }
}

impl StreamProject {
    pub fn new(logical: generic::Project<PlanRef>) -> Self {
        let input = logical.input.clone();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());

        let mut watermark_derivations = vec![];
        let mut nondecreasing_exprs = vec![];
        let mut watermark_columns = FixedBitSet::with_capacity(logical.exprs.len());
        for (expr_idx, expr) in logical.exprs.iter().enumerate() {
            match try_derive_watermark(expr) {
                WatermarkDerivation::Watermark(input_idx) => {
                    if input.watermark_columns().contains(input_idx) {
                        watermark_derivations.push((input_idx, expr_idx));
                        watermark_columns.insert(expr_idx);
                    }
                }
                WatermarkDerivation::Nondecreasing => {
                    nondecreasing_exprs.push(expr_idx);
                    watermark_columns.insert(expr_idx);
                }
                WatermarkDerivation::Constant => {
                    // XXX(rc): we can produce one watermark on each recovery for this case.
                }
                WatermarkDerivation::None => {}
            }
        }
        // Project executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream_with_logical(
            &logical,
            distribution,
            input.append_only(),
            input.emit_on_window_close(),
            watermark_columns,
        );
        StreamProject {
            base,
            logical,
            watermark_derivations,
            nondecreasing_exprs,
        }
    }

    pub fn as_logical(&self) -> &generic::Project<PlanRef> {
        &self.logical
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.logical.exprs
    }
}

impl PlanTreeNodeUnary for StreamProject {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}
impl_plan_tree_node_for_unary! {StreamProject}

impl StreamNode for StreamProject {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let (watermark_input_cols, watermark_output_cols) = self
            .watermark_derivations
            .iter()
            .map(|(i, o)| (*i as u32, *o as u32))
            .unzip();
        PbNodeBody::Project(ProjectNode {
            select_list: self
                .logical
                .exprs
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
            watermark_input_cols,
            watermark_output_cols,
            nondecreasing_exprs: self.nondecreasing_exprs.iter().map(|i| *i as _).collect(),
        })
    }
}

impl ExprRewritable for StreamProject {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
