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



use itertools::Itertools;
use risingwave_common::error::Result;

use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, BatchExpand, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamExpand, ToBatch, ToStream,
};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
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
    pub base: PlanBase,
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
}

impl PlanTreeNodeUnary for LogicalExpand {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.column_subsets().clone())
    }

    #[must_use]
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
        let (mut mapping, new_input_col_num) = input_col_change.into_parts();
        mapping.extend({
            mapping
                .iter()
                .map(|p| p.map(|i| i + new_input_col_num))
                .collect_vec()
        });
        mapping.push(Some(2 * new_input_col_num));

        (
            Self::new(input, column_subsets),
            ColIndexMapping::new(mapping),
        )
    }
}

impl_plan_tree_node_for_unary! {LogicalExpand}
impl_distill_by_unit!(LogicalExpand, core, "LogicalExpand");

impl ColPrunable for LogicalExpand {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        todo!("prune_col of LogicalExpand is not implemented yet.");
    }
}

impl ExprRewritable for LogicalExpand {}

impl PredicatePushdown for LogicalExpand {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // TODO: how to do predicate pushdown for Expand?
        //
        // let new_input = self.input.predicate_pushdown(predicate);
        // self.clone_with_input(new_input).into()

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
            .functional_dependency
            .add_functional_dependency_by_column_indices(&[0], &[1, 2]);

        let column_subsets = vec![vec![0, 1], vec![2]];
        let expand = LogicalExpand::create(values.into(), column_subsets);
        let fd = expand.functional_dependency().as_dependencies();
        assert_eq!(fd.len(), 1);
        assert_eq!(fd[0].from().ones().collect_vec(), &[0, 6]);
        assert_eq!(fd[0].to().ones().collect_vec(), &[1, 2]);
    }
}
