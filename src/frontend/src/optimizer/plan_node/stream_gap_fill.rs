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

use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::generic::GenericPlanNode;
use super::utils::TableCatalogBuilder;
use super::{
    ExprRewritable, ExprVisitable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, StreamNode,
    generic,
};
use crate::binder::BoundFillStrategy;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamGapFill` implements [`super::Stream`] to represent a gap-filling operation on a time
/// series in normal streaming mode (without EOWC semantics).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamGapFill {
    pub base: PlanBase<super::Stream>,
    core: generic::GapFill<PlanRef<Stream>>,
}

impl StreamGapFill {
    pub fn new(core: generic::GapFill<PlanRef<Stream>>) -> Self {
        let input = &core.input;

        // Verify that time_col is the ONLY stream key to ensure correctness.
        // GAP_FILL requires the time column to uniquely identify rows.
        let time_col_idx = core.time_col.index();
        let input_stream_key = input.expect_stream_key();
        assert!(
            input_stream_key.len() == 1 && input_stream_key[0] == time_col_idx,
            "GapFill requires the time column (index {}) to be the sole primary key. Found stream key: {:?}",
            time_col_idx,
            input_stream_key
        );

        // Use singleton distribution for normal streaming GapFill.
        // Similar to EOWC version, gap filling requires seeing all data
        // to correctly identify and fill gaps across time series.
        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::Single,
            input.stream_kind(),
            false, // does NOT provide EOWC semantics
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

    fn infer_state_table(&self) -> crate::TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        let out_schema = self.core.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        // For singleton distribution, use simplified primary key design:
        // Just use time column as the primary key for ordering
        let time_col_idx = self.time_col().index();
        tbl_builder.add_order_column(time_col_idx, OrderType::ascending());

        // Add is_filled flag column for gap fill state tracking
        tbl_builder.add_column(&Field::with_name(DataType::Boolean, "is_filled"));
        tbl_builder.build(vec![], 0)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamGapFill {
    fn input(&self) -> PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamGapFill }
impl_distill_by_unit!(StreamGapFill, core, "StreamGapFill");

impl StreamNode for StreamGapFill {
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

        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        NodeBody::GapFill(Box::new(GapFillNode {
            time_column_index: self.time_col().index() as u32,
            interval: Some(self.interval().to_expr_proto()),
            fill_columns: self
                .fill_strategies()
                .iter()
                .map(|strategy| strategy.target_col.index() as u32)
                .collect(),
            fill_strategies,
            state_table: Some(state_table),
        }))
    }
}

impl ExprRewritable<Stream> for StreamGapFill {
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

impl ExprVisitable for StreamGapFill {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}
