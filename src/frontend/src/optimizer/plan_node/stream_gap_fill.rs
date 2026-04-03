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
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::generic::GenericPlanNode;
use super::utils::TableCatalogBuilder;
use super::{
    ExprRewritable, ExprVisitable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, TryToStreamPb,
    generic,
};
use crate::binder::BoundFillStrategy;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::Distribution;
use crate::scheduler::SchedulerResult;
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
        let partition_indices: Vec<usize> =
            core.partition_by_cols.iter().map(|c| c.index()).collect();
        let distinct_partition_key_count = partition_indices
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(
            partition_indices.len(),
            distinct_partition_key_count,
            "stream gap fill expects canonicalized partition_by columns",
        );

        let dist = if core.partition_by_cols.is_empty() {
            Distribution::Single
        } else {
            Distribution::HashShard(partition_indices)
        };

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
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
            partition_by_cols: vec![],
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

    fn pointer_key_indices(&self) -> Vec<usize> {
        let time_col_idx = self.time_col().index();
        let partition_by_set: std::collections::HashSet<usize> = self
            .core
            .partition_by_cols
            .iter()
            .map(|c| c.index())
            .collect();
        let mut pointer_key_indices = vec![time_col_idx];
        for &sk_idx in self.input().expect_stream_key() {
            if sk_idx != time_col_idx
                && !partition_by_set.contains(&sk_idx)
                && !pointer_key_indices.contains(&sk_idx)
            {
                pointer_key_indices.push(sk_idx);
            }
        }
        pointer_key_indices
    }

    fn infer_state_table(&self) -> crate::TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();

        let out_schema = self.core.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        let time_col_idx = self.time_col().index();
        let input_schema = &out_schema;
        let pointer_key_indices = self.pointer_key_indices();

        // Add prev/next pointer columns for linked-list traversal.
        // Each pointer stores the intra-partition row identity:
        // (time column, upstream stream key columns excluding partition/time).
        for (i, &sk_idx) in pointer_key_indices.iter().enumerate() {
            tbl_builder.add_column(&Field::with_name(
                input_schema[sk_idx].data_type(),
                format!("prev_sk_{}", i),
            ));
        }
        for (i, &sk_idx) in pointer_key_indices.iter().enumerate() {
            tbl_builder.add_column(&Field::with_name(
                input_schema[sk_idx].data_type(),
                format!("next_sk_{}", i),
            ));
        }

        // PK: (partition_cols..., time_col, stream_key_cols...)
        // Dedup by column index since state table cannot have duplicate PK columns.
        let mut added_to_pk = std::collections::HashSet::new();
        for pc in &self.core.partition_by_cols {
            if added_to_pk.insert(pc.index()) {
                tbl_builder.add_order_column(pc.index(), OrderType::ascending());
            }
        }
        if added_to_pk.insert(time_col_idx) {
            tbl_builder.add_order_column(time_col_idx, OrderType::ascending());
        }
        for sk_idx in pointer_key_indices {
            if added_to_pk.insert(sk_idx) {
                tbl_builder.add_order_column(sk_idx, OrderType::ascending());
            }
        }

        let dist_key_indices: Vec<usize> = self
            .core
            .partition_by_cols
            .iter()
            .map(|c| c.index())
            .collect();
        tbl_builder.build(dist_key_indices, 0)
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

impl TryToStreamPb for StreamGapFill {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        use risingwave_pb::stream_plan::*;

        let fill_strategies: Vec<String> = self
            .fill_strategies()
            .iter()
            .map(|strategy| match strategy.strategy {
                crate::binder::FillStrategy::Locf => "locf".to_owned(),
                crate::binder::FillStrategy::Interpolate => "interpolate".to_owned(),
                crate::binder::FillStrategy::Null => "null".to_owned(),
            })
            .collect();

        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        Ok(NodeBody::GapFill(Box::new(GapFillNode {
            upstream_stream_key: self
                .input()
                .expect_stream_key()
                .iter()
                .map(|&idx| idx as u32)
                .collect(),
            pointer_key_indices: self
                .pointer_key_indices()
                .into_iter()
                .map(|idx| idx as u32)
                .collect(),
            time_column_index: self.time_col().index() as u32,
            interval: Some(self.interval().to_expr_proto_checked_pure(
                self.stream_kind().is_retract(),
                "gap filling interval",
            )?),
            fill_columns: self
                .fill_strategies()
                .iter()
                .map(|strategy| strategy.target_col.index() as u32)
                .collect(),
            fill_strategies,
            state_table: Some(state_table),
            partition_by_indices: self
                .core
                .partition_by_cols
                .iter()
                .map(|c| c.index() as u32)
                .collect(),
        })))
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
