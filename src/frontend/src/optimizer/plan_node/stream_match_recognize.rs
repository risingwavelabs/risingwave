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
use super::stream::prelude::*;
use super::utils::TableCatalogBuilder;
use super::{
    ExprRewritable, ExprVisitable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, TryToStreamPb,
    generic,
};
use crate::TableCatalog;
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamMatchRecognize` implements [`super::Stream`] for a SQL `MATCH_RECOGNIZE` (row pattern
/// recognition) operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamMatchRecognize {
    pub base: PlanBase<Stream>,
    core: generic::MatchRecognize<PlanRef<Stream>>,
}

impl StreamMatchRecognize {
    pub fn new(core: generic::MatchRecognize<PlanRef<Stream>>) -> Self {
        // ONE ROW PER MATCH emits one row per completed match over append-only input, so the output
        // is append-only. The output schema (partition cols + measures) differs from the input, so
        // start with empty watermark columns / monotonicity.
        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::Single,
            StreamKind::AppendOnly,
            false,
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );
        Self { base, core }
    }

    /// Per-partition buffered-row state table. Layout:
    ///   [ partition cols.. , seq (i64) , satisfied (varchar) , measure cols.. ]
    /// keyed by (partition cols, seq). The executor buffers one row per live input row (its
    /// satisfied pattern variables + pre-evaluated MEASURES) and restores the buffer from here on
    /// recovery. `seq` is a per-partition monotonic id; consumed rows are deleted after each drain.
    fn infer_state_table(&self) -> TableCatalog {
        let mut tbl_builder = TableCatalogBuilder::default();
        let out_fields = self.core.schema().fields().to_vec();
        let n_part = self.core.partition_by.len();

        // partition columns
        for f in &out_fields[0..n_part] {
            tbl_builder.add_column(f);
        }
        // seq
        tbl_builder.add_column(&Field::with_name(DataType::Int64, "seq"));
        // satisfied (comma-joined pattern variable names)
        tbl_builder.add_column(&Field::with_name(DataType::Varchar, "satisfied"));
        // measure columns
        for f in &out_fields[n_part..] {
            tbl_builder.add_column(f);
        }

        // pk: partition columns then seq
        for i in 0..n_part {
            tbl_builder.add_order_column(i, OrderType::ascending());
        }
        tbl_builder.add_order_column(n_part, OrderType::ascending());
        // read_prefix_len_hint = 0: recovery does a full (empty-prefix) scan to discover partitions,
        // so we must not assert a partition-length prefix on iteration.
        tbl_builder.build(vec![], 0)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamMatchRecognize {
    fn input(&self) -> PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamMatchRecognize }
impl_distill_by_unit!(StreamMatchRecognize, core, "StreamMatchRecognize");

impl TryToStreamPb for StreamMatchRecognize {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        use risingwave_pb::stream_plan::*;

        let retract = self.stream_kind().is_retract();

        // PARTITION BY / ORDER BY were validated to be plain columns in `to_stream`.
        let partition_by = self
            .core
            .partition_key_indices()
            .expect("partition keys validated to be columns")
            .into_iter()
            .map(|i| i as u32)
            .collect();
        let order_by = self
            .core
            .order_key_indices()
            .expect("order keys validated to be columns")
            .into_iter()
            .map(|i| i as u32)
            .collect();

        let measures = self
            .core
            .measures
            .iter()
            .map(|m| m.expr.to_expr_proto_checked_pure(retract, "match_recognize measure"))
            .collect::<crate::error::Result<Vec<_>>>()?;
        let measure_names = self.core.measures.iter().map(|m| m.name.clone()).collect();

        let define_symbols = self.core.defines.iter().map(|d| d.symbol.clone()).collect();
        let define_conditions = self
            .core
            .defines
            .iter()
            .map(|d| {
                d.definition
                    .to_expr_proto_checked_pure(retract, "match_recognize define")
            })
            .collect::<crate::error::Result<Vec<_>>>()?;

        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        Ok(NodeBody::MatchRecognize(MatchRecognizeNode {
            partition_by,
            order_by,
            measures,
            measure_names,
            define_symbols,
            define_conditions,
            pattern: format!("{}", self.core.pattern),
            state_table: Some(state_table),
            after_match_skip: match &self.core.after_match_skip {
                Some(risingwave_sqlparser::ast::AfterMatchSkip::ToNextRow) => "to_next_row",
                _ => "past_last_row",
            }
            .to_owned(),
        }))
    }
}

impl ExprRewritable<Stream> for StreamMatchRecognize {
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

impl ExprVisitable for StreamMatchRecognize {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v)
    }
}
