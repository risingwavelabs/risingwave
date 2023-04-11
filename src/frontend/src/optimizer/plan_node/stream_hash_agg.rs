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
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::ExprRewriter;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHashAgg {
    pub base: PlanBase,
    logical: generic::Agg<PlanRef>,

    /// An optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution.
    vnode_col_idx: Option<usize>,

    /// The index of `count(*)` in `agg_calls`.
    row_count_idx: usize,
}

impl StreamHashAgg {
    pub fn new(
        logical: generic::Agg<PlanRef>,
        vnode_col_idx: Option<usize>,
        row_count_idx: usize,
    ) -> Self {
        assert_eq!(logical.agg_calls[row_count_idx], PlanAggCall::count_star());

        let base = PlanBase::new_logical_with_core(&logical);
        let ctx = base.ctx;
        let pk_indices = base.logical_pk;
        let schema = base.schema;
        let input = logical.input.clone();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _) => logical
                .i2o_col_mapping()
                .rewrite_provided_distribution(input_dist),
            d => d.clone(),
        };

        let mut watermark_columns = FixedBitSet::with_capacity(schema.len());
        // Watermark column(s) must be in group key.
        for (idx, input_idx) in logical.group_key.iter().enumerate() {
            if input.watermark_columns().contains(*input_idx) {
                watermark_columns.insert(idx);
            }
        }

        // Hash agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            schema,
            pk_indices,
            base.functional_dependency,
            dist,
            false,
            watermark_columns,
        );
        StreamHashAgg {
            base,
            logical,
            vnode_col_idx,
            row_count_idx,
        }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.logical.agg_calls
    }

    pub fn group_key(&self) -> &[usize] {
        &self.logical.group_key
    }

    pub(crate) fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.logical.i2o_col_mapping()
    }
}

impl fmt::Display for StreamHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = if self.input().append_only() {
            f.debug_struct("StreamAppendOnlyHashAgg")
        } else {
            f.debug_struct("StreamHashAgg")
        };
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

impl PlanTreeNodeUnary for StreamHashAgg {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let logical = generic::Agg {
            input,
            ..self.logical.clone()
        };
        Self::new(logical, self.vnode_col_idx, self.row_count_idx)
    }
}
impl_plan_tree_node_for_unary! { StreamHashAgg }

impl StreamNode for StreamHashAgg {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        let (result_table, agg_states, distinct_dedup_tables) =
            self.logical.infer_tables(&self.base, self.vnode_col_idx);

        PbNodeBody::HashAgg(HashAggNode {
            group_key: self.group_key().iter().map(|idx| *idx as u32).collect(),
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
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

impl ExprRewritable for StreamHashAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical, self.vnode_col_idx, self.row_count_idx).into()
    }
}
