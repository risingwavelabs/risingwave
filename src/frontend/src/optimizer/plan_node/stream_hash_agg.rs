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
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::stream::prelude::*;
use super::utils::{Distill, childless_record, plan_node_name, watermark_pretty};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, IndexSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHashAgg {
    pub base: PlanBase<Stream>,
    core: generic::Agg<PlanRef>,

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
        core: generic::Agg<PlanRef>,
        vnode_col_idx: Option<usize>,
        row_count_idx: usize,
    ) -> Self {
        Self::new_with_eowc(core, vnode_col_idx, row_count_idx, false)
    }

    pub fn new_with_eowc(
        core: generic::Agg<PlanRef>,
        vnode_col_idx: Option<usize>,
        row_count_idx: usize,
        emit_on_window_close: bool,
    ) -> Self {
        assert_eq!(core.agg_calls[row_count_idx], PlanAggCall::count_star());

        let input = core.input.clone();
        let input_dist = input.distribution();
        let dist = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(input_dist);

        let mut watermark_columns = WatermarkColumns::new();
        let mut window_col_idx = None;
        let mapping = core.i2o_col_mapping();
        if emit_on_window_close {
            let window_col = core
                .eowc_window_column(input.watermark_columns())
                .expect("checked in `to_eowc_version`");
            // EOWC HashAgg only propagate one watermark column, the window column.
            watermark_columns.insert(
                mapping.map(window_col),
                input.watermark_columns().get_group(window_col).unwrap(),
            );
            window_col_idx = Some(window_col);
        } else {
            for idx in core.group_key.indices() {
                if let Some(wtmk_group) = input.watermark_columns().get_group(idx) {
                    // Non-EOWC `StreamHashAgg` simply forwards the watermark messages from the input.
                    watermark_columns.insert(mapping.map(idx), wtmk_group);
                }
            }
        }

        // Hash agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            emit_on_window_close, // in EOWC mode, we produce append only output
            emit_on_window_close,
            watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );
        StreamHashAgg {
            base,
            core,
            vnode_col_idx,
            row_count_idx,
            emit_on_window_close,
            window_col_idx,
        }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }

    pub fn group_key(&self) -> &IndexSet {
        &self.core.group_key
    }

    pub(crate) fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.core.i2o_col_mapping()
    }

    // TODO(rc): It'll be better to force creation of EOWC version through `new`, especially when we
    // optimize for 2-phase EOWC aggregation later.
    pub fn to_eowc_version(&self) -> Result<PlanRef> {
        let input = self.input();

        // check whether the group by columns are valid
        let _ = self.core.eowc_window_column(input.watermark_columns())?;

        Ok(Self::new_with_eowc(
            self.core.clone(),
            self.vnode_col_idx,
            self.row_count_idx,
            true,
        )
        .into())
    }
}

impl Distill for StreamHashAgg {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut vec = self.core.fields_pretty();
        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }
        childless_record(
            plan_node_name!(
                "StreamHashAgg",
                { "append_only", self.input().append_only() },
                { "eowc", self.emit_on_window_close },
            ),
            vec,
        )
    }
}

impl PlanTreeNodeUnary for StreamHashAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let logical = generic::Agg {
            input,
            ..self.core.clone()
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
        let (intermediate_state_table, agg_states, distinct_dedup_tables) =
            self.core
                .infer_tables(&self.base, self.vnode_col_idx, self.window_col_idx);

        PbNodeBody::HashAgg(Box::new(HashAggNode {
            group_key: self.group_key().to_vec_as_u32(),
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
            intermediate_state_table: Some(
                intermediate_state_table
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
            emit_on_window_close: self.base.emit_on_window_close(),
            version: PbAggNodeVersion::LATEST as _,
        }))
    }
}

impl ExprRewritable for StreamHashAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new_with_eowc(
            core,
            self.vnode_col_idx,
            self.row_count_idx,
            self.emit_on_window_close,
        )
        .into()
    }
}

impl ExprVisitable for StreamHashAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
