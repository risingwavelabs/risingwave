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

use core::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::{
    gen_filter_and_pushdown, generic, BatchGroupTopN, ColPrunable, ColumnPruningContext,
    ExprRewritable, LogicalProject, LogicalTopN, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, PredicatePushdownContext, RewriteStreamContext, StreamDedup,
    StreamGroupTopN, ToBatch, ToStream, ToStreamContext,
};
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::Condition;

/// [`LogicalDedup`] deduplicates data on specific columns. It is now used in `DISTINCT ON` without
/// an `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalDedup {
    pub base: PlanBase,
    core: generic::Dedup<PlanRef>,
}

impl LogicalDedup {
    pub fn new(input: PlanRef, dedup_cols: Vec<usize>) -> Self {
        let base = PlanBase::new_logical(
            input.ctx(),
            input.schema().clone(),
            dedup_cols.clone(),
            input.functional_dependency().clone(),
        );
        let core = generic::Dedup { input, dedup_cols };
        LogicalDedup { base, core }
    }

    pub fn dedup_cols(&self) -> &[usize] {
        &self.core.dedup_cols
    }
}

impl PlanTreeNodeUnary for LogicalDedup {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.dedup_cols().to_vec())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (
            Self::new(
                input,
                self.dedup_cols()
                    .iter()
                    .map(|idx| input_col_change.map(*idx))
                    .collect_vec(),
            ),
            input_col_change,
        )
    }
}

impl_plan_tree_node_for_unary! {LogicalDedup}

impl PredicatePushdown for LogicalDedup {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToStream for LogicalDedup {
    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (logical, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((logical.into(), out_col_change))
    }

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let input = self.input().to_stream(ctx)?;
        let input = RequiredDist::hash_shard(self.dedup_cols())
            .enforce_if_not_satisfies(input, &Order::any())?;
        if input.append_only() {
            // `LogicalDedup` is transformed to `StreamDedup` only when the input is append-only.
            let mut logical_dedup = self.core.clone();
            logical_dedup.input = input;
            Ok(StreamDedup::new(logical_dedup).into())
        } else {
            // If the input is not append-only, we use a `StreamGroupTopN` with the limit being 1.
            let logical_top_n = LogicalTopN::with_group(
                input,
                1,
                0,
                false,
                Order::default(),
                self.dedup_cols().to_vec(),
            );
            Ok(StreamGroupTopN::new(logical_top_n, None).into())
        }
    }
}

impl ToBatch for LogicalDedup {
    fn to_batch(&self) -> Result<PlanRef> {
        let input = self.input().to_batch()?;
        let logical_top_n = LogicalTopN::with_group(
            input,
            1,
            0,
            false,
            Order::default(),
            self.dedup_cols().to_vec(),
        );
        Ok(BatchGroupTopN::new(logical_top_n).into())
    }
}

impl ExprRewritable for LogicalDedup {}

impl ColPrunable for LogicalDedup {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_required_bitset = FixedBitSet::from_iter(required_cols.iter().copied());
        let dedup_required_bitset = {
            let mut dedup_required_bitset = FixedBitSet::with_capacity(self.input().schema().len());
            self.dedup_cols()
                .iter()
                .for_each(|idx| dedup_required_bitset.insert(*idx));
            dedup_required_bitset
        };
        let input_required_cols = {
            let mut tmp = input_required_bitset;
            tmp.union_with(&dedup_required_bitset);
            tmp.ones().collect_vec()
        };
        let mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );

        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let logical_dedup = Self::new(new_input, self.dedup_cols().to_vec()).into();

        if input_required_cols == required_cols {
            logical_dedup
        } else {
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = logical_dedup.schema().len();
            LogicalProject::with_mapping(
                logical_dedup,
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl fmt::Display for LogicalDedup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.core.fmt_with_name(f, "LogicalDedup")
    }
}
