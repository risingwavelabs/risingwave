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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_expr::bail;
use risingwave_sqlparser::ast::{AfterMatchSkip, MatchRecognizePattern, RowsPerMatch};

use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, ExprVisitable, Logical, LogicalFilter,
    LogicalPlanRef as PlanRef, LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown,
    PredicatePushdownContext, ToBatch, ToStream, ToStreamContext, generic,
};
use super::generic::GenericPlanRef;
use super::stream::StreamPlanNodeMetadata;
use crate::binder::{BoundMeasure, BoundSymbolDefinition, MeasureSlotKind};
use crate::error::Result;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalMatchRecognize` implements [`super::Logical`] for a SQL `MATCH_RECOGNIZE` (row pattern
/// recognition) operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalMatchRecognize {
    pub base: PlanBase<super::Logical>,
    core: generic::MatchRecognize<PlanRef>,
}

impl LogicalMatchRecognize {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: PlanRef,
        partition_by: Vec<ExprImpl>,
        order_by: Vec<ExprImpl>,
        measures: Vec<BoundMeasure>,
        rows_per_match: Option<RowsPerMatch>,
        after_match_skip: Option<AfterMatchSkip>,
        pattern: MatchRecognizePattern,
        defines: Vec<BoundSymbolDefinition>,
    ) -> Self {
        let core = generic::MatchRecognize {
            input,
            partition_by,
            order_by,
            measures,
            rows_per_match,
            after_match_skip,
            pattern,
            defines,
        };
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    /// The set of input columns referenced by any expression in the clause.
    fn input_required_cols(&self) -> FixedBitSet {
        let input_col_num = self.core.input.schema().len();
        let mut required = FixedBitSet::with_capacity(input_col_num);
        for e in &self.core.partition_by {
            required.union_with(&e.collect_input_refs(input_col_num));
        }
        for e in &self.core.order_by {
            required.union_with(&e.collect_input_refs(input_col_num));
        }
        // Measure expressions are over the synthetic per-match row; the real input columns they read
        // are recorded in the slots.
        for m in &self.core.measures {
            for slot in &m.slots {
                if !matches!(slot.kind, MeasureSlotKind::Classifier) {
                    required.insert(slot.col_idx);
                }
            }
        }
        for d in &self.core.defines {
            required.union_with(&d.definition.collect_input_refs(input_col_num));
        }
        required
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalMatchRecognize {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.core.partition_by.clone(),
            self.core.order_by.clone(),
            self.core.measures.clone(),
            self.core.rows_per_match.clone(),
            self.core.after_match_skip.clone(),
            self.core.pattern.clone(),
            self.core.defines.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalMatchRecognize }
impl_distill_by_unit!(LogicalMatchRecognize, core, "LogicalMatchRecognize");

impl ColPrunable for LogicalMatchRecognize {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // Prune the input down to the columns the clause's expressions actually reference.
        let input_col_num = self.core.input.schema().len();
        let input_required = self.input_required_cols();
        let input_required_cols: Vec<_> = input_required.ones().collect();

        let mut col_index_mapping =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_col_num);

        let mut new_core = self.core.clone();
        new_core.input = self.core.input.prune_col(&input_required_cols, ctx);
        new_core.rewrite_with_col_index_mapping(&mut col_index_mapping);

        let node: PlanRef = Self {
            base: PlanBase::new_logical_with_core(&new_core),
            core: new_core,
        }
        .into();

        // The node's own output is (partition cols + measures); project if the caller wants a subset.
        let output_col_num = self.schema().len();
        if required_cols == (0..output_col_num).collect_vec() {
            node
        } else {
            LogicalProject::with_mapping(
                node,
                ColIndexMapping::with_remaining_columns(required_cols, output_col_num),
            )
            .into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalMatchRecognize {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: PlanBase::new_logical_with_core(&core),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalMatchRecognize {
    fn visit_exprs(&self, v: &mut dyn crate::expr::ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalMatchRecognize {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // Output columns are computed (partition/measures), so do not push predicates through.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalMatchRecognize {
    fn to_batch(&self) -> Result<super::BatchPlanRef> {
        bail!("BatchMatchRecognize is not implemented yet")
    }
}

impl ToStream for LogicalMatchRecognize {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<super::StreamPlanRef> {
        use super::StreamMatchRecognize;

        // v1 restrictions: PARTITION BY / ORDER BY must be plain columns, PARTITION BY non-empty.
        if self.core.partition_key_indices().is_none() || self.core.order_key_indices().is_none() {
            bail!(
                "MATCH_RECOGNIZE currently supports only plain column references in PARTITION BY and ORDER BY"
            );
        }
        if self
            .core
            .partition_key_indices()
            .expect("checked above")
            .is_empty()
        {
            bail!("MATCH_RECOGNIZE currently requires a non-empty PARTITION BY");
        }
        let order_indices = self.core.order_key_indices().expect("checked above");
        let Some(&time_col) = order_indices.first() else {
            bail!("MATCH_RECOGNIZE requires an ORDER BY clause");
        };

        let stream_input = self.input().to_stream(ctx)?;
        // Event-time contract: the executor buffers rows and finalises matches as the watermark on
        // the leading ORDER BY column advances, so that column must carry a watermark. This mirrors
        // Flink requiring a rowtime attribute on ORDER BY.
        if !stream_input.watermark_columns().contains(time_col) {
            bail!(
                "MATCH_RECOGNIZE requires a watermark on the leading ORDER BY column for streaming"
            );
        }
        let core = generic::MatchRecognize {
            input: stream_input,
            partition_by: self.core.partition_by.clone(),
            order_by: self.core.order_by.clone(),
            measures: self.core.measures.clone(),
            rows_per_match: self.core.rows_per_match.clone(),
            after_match_skip: self.core.after_match_skip.clone(),
            pattern: self.core.pattern.clone(),
            defines: self.core.defines.clone(),
        };
        Ok(StreamMatchRecognize::new(core).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut super::convert::RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.core.input.logical_rewrite_for_stream(ctx)?;
        let mut new_core = self.core.clone();
        new_core.input = input;
        let mut mapping = input_col_change;
        new_core.rewrite_with_col_index_mapping(&mut mapping);
        let node = Self {
            base: PlanBase::new_logical_with_core(&new_core),
            core: new_core,
        };
        // Output columns (partition + measures) are produced fresh by this node, so downstream sees
        // an identity mapping over this node's own output schema.
        let out_col_change = ColIndexMapping::identity(node.schema().len());
        Ok((node.into(), out_col_change))
    }
}
