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

use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::generic::GenericPlanNode;
use super::utils::TableCatalogBuilder;
use super::{
    ExprRewritable, ExprVisitable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, StreamNode,
    generic,
};
use crate::TableCatalog;
use crate::binder::BoundFillStrategy;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamEowcGapFill` implements [`super::Stream`] to represent a gap-filling operation on a time
/// series in streaming mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamEowcGapFill {
    pub base: PlanBase<super::Stream>,
    core: generic::GapFill<PlanRef<Stream>>,
}

impl StreamEowcGapFill {
    pub fn new(core: generic::GapFill<PlanRef<Stream>>) -> Self {
        let input = &core.input;

        // Verify that time_col is part of the upstream stream key to ensure
        // that there are no duplicate rows for the same time point.
        let time_col_idx = core.time_col.index();
        let input_stream_key = input.expect_stream_key();
        assert!(
            input_stream_key.contains(&time_col_idx),
            "GapFill time column (index {}) must be part of the upstream stream key {:?} to avoid logic errors with duplicate rows",
            time_col_idx,
            input_stream_key
        );

        // Verify that a watermark is defined on the gap fill column.
        let input_watermark_cols = input.watermark_columns();
        assert!(
            input_watermark_cols.contains(time_col_idx),
            "GapFill time column (index {}) must have a watermark defined for EOWC mode",
            time_col_idx
        );

        // Force singleton distribution for GapFill operations.
        // GapFill requires access to all data across time ranges to correctly identify and fill gaps, so that missing intervals can be detected and filled appropriately.
        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::Single,
            input.stream_kind(),
            true, // provides EOWC semantics
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );
        Self { base, core }
    }

    // Legacy constructor for backward compatibility
    pub fn new_with_args(
        input: PlanRef<Stream>,
        time_col: InputRef,
        interval: ExprImpl,
        fill_strategies: Vec<BoundFillStrategy>,
    ) -> Self {
        let core = generic::GapFill {
            input,
            time_col,
            interval,
            fill_strategies,
        };
        Self::new(core)
    }

    pub fn time_col(&self) -> &InputRef {
        &self.core.time_col
    }

    pub fn interval(&self) -> &ExprImpl {
        &self.core.interval
    }

    pub fn fill_strategies(&self) -> &[BoundFillStrategy] {
        &self.core.fill_strategies
    }

    fn infer_buffer_table(&self) -> TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        let out_schema = self.core.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        // Just use time column as the primary key since SortBuffer requires it for ordering
        let time_col_idx = self.time_col().index();
        tbl_builder.add_order_column(time_col_idx, OrderType::ascending());

        tbl_builder.build(vec![], 0)
    }

    fn infer_prev_row_table(&self) -> TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        for field in self.core.schema().fields() {
            tbl_builder.add_column(field);
        }

        if !self.core.schema().fields().is_empty() {
            tbl_builder.add_order_column(0, OrderType::ascending());
        }

        tbl_builder.build(vec![], 0)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamEowcGapFill {
    fn input(&self) -> PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamEowcGapFill }
impl_distill_by_unit!(StreamEowcGapFill, core, "StreamEowcGapFill");

impl StreamNode for StreamEowcGapFill {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        use risingwave_pb::stream_plan::*;

        let fill_strategies: Vec<String> = self
            .fill_strategies()
            .iter()
            .map(|strategy| match strategy.strategy {
                crate::binder::FillStrategy::Locf => "locf".to_string(),
                crate::binder::FillStrategy::Interpolate => "interpolate".to_string(),
                crate::binder::FillStrategy::Null => "null".to_string(),
            })
            .collect();

        let buffer_table = self
            .infer_buffer_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        let prev_row_table = self
            .infer_prev_row_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        NodeBody::EowcGapFill(EowcGapFillNode {
            time_column_index: self.time_col().index() as u32,
            interval: Some(self.interval().to_expr_proto()),
            fill_columns: self
                .fill_strategies()
                .iter()
                .map(|strategy| strategy.target_col.index() as u32)
                .collect(),
            fill_strategies,
            buffer_table: Some(buffer_table),
            prev_row_table: Some(prev_row_table),
        })
    }
}

impl ExprRewritable<Stream> for StreamEowcGapFill {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef<Stream> {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for StreamEowcGapFill {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}
