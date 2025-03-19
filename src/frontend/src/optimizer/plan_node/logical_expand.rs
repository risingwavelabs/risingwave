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

use super::generic::GenericPlanRef;
use super::utils::impl_distill_by_unit;
use super::{
    BatchExpand, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, StreamExpand, ToBatch, ToStream, gen_filter_and_pushdown, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalProject, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalExpand`] expands one row multiple times according to `column_subsets` and also keeps
/// original columns of input. It can be used to implement distinct aggregation and group set.
///
/// This is the schema of `LogicalExpand`:
/// | expanded columns(i.e. some columns are set to null) | original columns of input | flag |.
///
/// Aggregates use expanded columns as their arguments and original columns for their filter. `flag`
/// is used to distinguish between different `subset`s in `column_subsets`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalExpand {
    pub base: PlanBase<Logical>,
    core: generic::Expand<PlanRef>,
}

impl LogicalExpand {
    pub fn new(input: PlanRef, column_subsets: Vec<Vec<usize>>) -> Self {
        for key in column_subsets.iter().flatten() {
            assert!(*key < input.schema().len());
        }

        let core = generic::Expand {
            column_subsets,
            input,
        };
        let base = PlanBase::new_logical_with_core(&core);

        LogicalExpand { base, core }
    }

    pub fn create(input: PlanRef, column_subsets: Vec<Vec<usize>>) -> PlanRef {
        Self::new(input, column_subsets).into()
    }

    pub fn column_subsets(&self) -> &Vec<Vec<usize>> {
        &self.core.column_subsets
    }

    pub fn decompose(self) -> (PlanRef, Vec<Vec<usize>>) {
        self.core.decompose()
    }
}

impl PlanTreeNodeUnary for LogicalExpand {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.column_subsets().clone())
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let column_subsets = self
            .column_subsets()
            .iter()
            .map(|subset| {
                subset
                    .iter()
                    .filter_map(|i| input_col_change.try_map(*i))
                    .collect_vec()
            })
            .collect_vec();

        let old_out_len = self.schema().len();
        let old_in_len = self.input().schema().len();
        let new_in_len = input.schema().len();
        assert_eq!(
            old_out_len,
            old_in_len * 2 + 1 // expanded input cols + real input cols + flag
        );

        let mut mapping = Vec::with_capacity(old_out_len);
        // map the expanded input columns
        for i in 0..old_in_len {
            mapping.push(input_col_change.try_map(i));
        }
        // map the real input columns
        for i in 0..old_in_len {
            mapping.push(
                input_col_change
                    .try_map(i)
                    .map(|x| x + new_in_len /* # of new expanded input cols */),
            );
        }
        // map the flag column
        mapping.push(Some(2 * new_in_len));

        let expand = Self::new(input, column_subsets);
        let output_col_num = expand.schema().len();
        (expand, ColIndexMapping::new(mapping, output_col_num))
    }
}

impl_plan_tree_node_for_unary! {LogicalExpand}
impl_distill_by_unit!(LogicalExpand, core, "LogicalExpand");

impl ColPrunable for LogicalExpand {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // No pruning.
        let input_required_cols = (0..self.input().schema().len()).collect_vec();
        LogicalProject::with_out_col_idx(
            self.clone_with_input(self.input().prune_col(&input_required_cols, ctx))
                .into(),
            required_cols.iter().cloned(),
        )
        .into()
    }
}

impl ExprRewritable for LogicalExpand {}

impl ExprVisitable for LogicalExpand {}

impl PredicatePushdown for LogicalExpand {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalExpand {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        Ok(BatchExpand::new(new_logical).into())
    }
}

impl ToStream for LogicalExpand {
    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (expand, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((expand.into(), out_col_change))
    }

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let new_input = self.input().to_stream(ctx)?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        Ok(StreamExpand::new(new_logical).into())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::{LogicalExpand, LogicalValues};

    // TODO(Wenzhuo): change this test according to expand's new definition.
    #[tokio::test]
    async fn fd_derivation_expand() {
        // input: [v1, v2, v3]
        // FD: v1 --> { v2, v3 }
        // output: [v1, v2, v3, flag],
        // FD: { v1, flag } --> { v2, v3 }
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let mut values = LogicalValues::new(vec![], Schema { fields }, ctx);
        values
            .base
            .functional_dependency_mut()
            .add_functional_dependency_by_column_indices(&[0], &[1, 2]);

        let column_subsets = vec![vec![0, 1], vec![2]];
        let expand = LogicalExpand::create(values.into(), column_subsets);
        let fd = expand.functional_dependency().as_dependencies();
        assert_eq!(fd.len(), 1);
        assert_eq!(fd[0].from().ones().collect_vec(), &[0, 6]);
        assert_eq!(fd[0].to().ones().collect_vec(), &[1, 2]);
    }
}
