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
use risingwave_common::catalog::FieldDisplay;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::ProjectNode;

use super::{ExprRewritable, LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{try_derive_watermark, Expr, ExprImpl, ExprRewriter};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// `StreamProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamProject {
    pub base: PlanBase,
    logical: LogicalProject,
    /// All the watermark derivations, (input_column_index, output_column_index). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: Vec<(usize, usize)>,
}

impl fmt::Display for StreamProject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamProject");
        self.logical.fmt_fields_with_builder(&mut builder);
        if !self.watermark_derivations.is_empty() {
            builder.field(
                "output_watermarks",
                &self
                    .watermark_derivations
                    .iter()
                    .map(|(_, idx)| FieldDisplay(self.schema().fields.get(*idx).unwrap()))
                    .collect_vec(),
            );
        };
        builder.finish()
    }
}

impl StreamProject {
    pub fn new(logical: LogicalProject) -> Self {
        let ctx = logical.base.ctx.clone();
        let input = logical.input();
        let pk_indices = logical.base.logical_pk.to_vec();
        let schema = logical.schema().clone();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());

        let mut watermark_derivations = vec![];
        let mut watermark_columns = FixedBitSet::with_capacity(schema.len());
        for (expr_idx, expr) in logical.exprs().iter().enumerate() {
            if let Some(input_idx) = try_derive_watermark(expr) {
                if input.watermark_columns().contains(input_idx) {
                    watermark_derivations.push((input_idx, expr_idx));
                    watermark_columns.insert(expr_idx);
                }
            }
        }
        // Project executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream(
            ctx,
            schema,
            pk_indices,
            logical.functional_dependency().clone(),
            distribution,
            logical.input().append_only(),
            watermark_columns,
        );
        StreamProject {
            base,
            logical,
            watermark_derivations,
        }
    }

    pub fn as_logical(&self) -> &LogicalProject {
        &self.logical
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        self.logical.exprs()
    }
}

impl PlanTreeNodeUnary for StreamProject {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! {StreamProject}

impl StreamNode for StreamProject {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::Project(ProjectNode {
            select_list: self
                .logical
                .exprs()
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
            watermark_input_key: self
                .watermark_derivations
                .iter()
                .map(|(x, _)| *x as u32)
                .collect(),
            watermark_output_key: self
                .watermark_derivations
                .iter()
                .map(|(_, y)| *y as u32)
                .collect(),
        })
    }
}

impl ExprRewritable for StreamProject {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_project()
                .unwrap()
                .clone(),
        )
        .into()
    }
}
