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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::utils::formatter_debug_plan_node;
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

    /// Whether to emit output only when the window is closed by watermark.
    emit_on_window_close: bool,

    /// The watermark column that Emit-On-Window-Close behavior is based on.
    window_col_idx: Option<usize>,
}

impl StreamHashAgg {
    pub fn new(
        logical: generic::Agg<PlanRef>,
        vnode_col_idx: Option<usize>,
        row_count_idx: usize,
    ) -> Self {
        Self::new_with_eowc(logical, vnode_col_idx, row_count_idx, false)
    }

    pub fn new_with_eowc(
        logical: generic::Agg<PlanRef>,
        vnode_col_idx: Option<usize>,
        row_count_idx: usize,
        emit_on_window_close: bool,
    ) -> Self {
        assert_eq!(logical.agg_calls[row_count_idx], PlanAggCall::count_star());

        let input = logical.input.clone();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(_, _) => logical
                .i2o_col_mapping()
                .rewrite_provided_distribution(input_dist),
            d => d.clone(),
        };

        let mut watermark_columns = FixedBitSet::with_capacity(logical.output_len());
        let mut window_col_idx = None;
        let mapping = logical.i2o_col_mapping();
        if emit_on_window_close {
            let wtmk_group_key = logical.watermark_group_key(input.watermark_columns());
            assert!(wtmk_group_key.len() == 1); // checked in `to_eowc_version`
            window_col_idx = Some(wtmk_group_key[0]);
            // EOWC HashAgg only produce one watermark column, i.e. the window column
            watermark_columns.insert(mapping.map(wtmk_group_key[0]));
        } else {
            for idx in logical.group_key.ones() {
                if input.watermark_columns().contains(idx) {
                    watermark_columns.insert(mapping.map(idx));
                }
            }
        }

        // Hash agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_logical(
            &logical,
            dist,
            false,
            emit_on_window_close,
            watermark_columns,
        );
        StreamHashAgg {
            base,
            logical,
            vnode_col_idx,
            row_count_idx,
            emit_on_window_close,
            window_col_idx,
        }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.logical.agg_calls
    }

    pub fn group_key(&self) -> &FixedBitSet {
        &self.logical.group_key
    }

    pub(crate) fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.logical.i2o_col_mapping()
    }

    // TODO(rc): It'll be better to force creation of EOWC version through `new`, especially when we
    // optimize for 2-phase EOWC aggregation later.
    pub fn to_eowc_version(&self) -> Result<PlanRef> {
        let input = self.input();
        let wtmk_group_key = self.logical.watermark_group_key(input.watermark_columns());

        if wtmk_group_key.is_empty() || wtmk_group_key.len() > 1 {
            return Err(ErrorCode::NotSupported(
                "The query cannot be executed in Emit-On-Window-Close mode.".to_string(),
                "Please make sure there is one and only one watermark column in GROUP BY"
                    .to_string(),
            )
            .into());
        }

        Ok(Self::new_with_eowc(
            self.logical.clone(),
            self.vnode_col_idx,
            self.row_count_idx,
            true,
        )
        .into())
    }
}

impl fmt::Display for StreamHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = formatter_debug_plan_node!(
            f, "StreamHashAgg"
            , { "in_append_only", self.input().append_only() }
            , { "eowc", self.emit_on_window_close }
        );
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
        Self::new_with_eowc(
            logical,
            self.vnode_col_idx,
            self.row_count_idx,
            self.emit_on_window_close,
        )
    }
}
impl_plan_tree_node_for_unary! { StreamHashAgg }

impl StreamNode for StreamHashAgg {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        let (result_table, agg_states, distinct_dedup_tables) =
            self.logical
                .infer_tables(&self.base, self.vnode_col_idx, self.window_col_idx);

        PbNodeBody::HashAgg(HashAggNode {
            group_key: self.group_key().ones().map(|idx| idx as u32).collect(),
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
            emit_on_window_close: self.base.emit_on_window_close,
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
        Self::new_with_eowc(
            logical,
            self.vnode_col_idx,
            self.row_count_idx,
            self.emit_on_window_close,
        )
        .into()
    }
}
