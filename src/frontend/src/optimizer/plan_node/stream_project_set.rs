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
use risingwave_pb::stream_plan::ProjectSetNode;

use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{try_derive_watermark, ExprRewriter};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamProjectSet {
    pub base: PlanBase,
    logical: generic::ProjectSet<PlanRef>,
}

impl StreamProjectSet {
    pub fn new(logical: generic::ProjectSet<PlanRef>) -> Self {
        let input = logical.input.clone();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());

        let mut watermark_columns = FixedBitSet::with_capacity(logical.output_len());
        for (expr_idx, expr) in logical.select_list.iter().enumerate() {
            if let Some(input_idx) = try_derive_watermark(expr) {
                if input.watermark_columns().contains(input_idx) {
                    // The first column of ProjectSet is `projected_row_id`.
                    watermark_columns.insert(expr_idx + 1);
                }
            }
        }

        // ProjectSet executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream_with_logical(
            &logical,
            distribution,
            input.append_only(),
            watermark_columns,
        );
        StreamProjectSet { base, logical }
    }
}

impl fmt::Display for StreamProjectSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamProjectSet")
    }
}

impl PlanTreeNodeUnary for StreamProjectSet {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { StreamProjectSet }

impl StreamNode for StreamProjectSet {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::ProjectSet(ProjectSetNode {
            select_list: self
                .logical
                .select_list
                .iter()
                .map(|select_item| select_item.to_project_set_select_item_proto())
                .collect_vec(),
        })
    }
}

impl ExprRewritable for StreamProjectSet {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
