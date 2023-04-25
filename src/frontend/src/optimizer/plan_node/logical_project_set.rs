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
use risingwave_common::error::Result;

use super::{
    gen_filter_and_pushdown, generic, BatchProjectSet, ColPrunable, ExprRewritable, LogicalProject,
    PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, StreamProjectSet, ToBatch, ToStream,
};
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprVisitor, FunctionCall, InputRef, TableFunction,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    CollectInputRef, ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalProjectSet` projects one row multiple times according to `select_list`.
///
/// Different from `Project`, it supports [`TableFunction`](crate::expr::TableFunction)s.
/// See also [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem) for examples.
///
/// To have a pk, it has a hidden column `projected_row_id` at the beginning. The implementation of
/// `LogicalProjectSet` is highly similar to [`LogicalProject`], except for the additional hidden
/// column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalProjectSet {
    pub base: PlanBase,
    core: generic::ProjectSet<PlanRef>,
}

impl LogicalProjectSet {
    pub fn new(input: PlanRef, select_list: Vec<ExprImpl>) -> Self {
        assert!(
            select_list.iter().any(|e| e.has_table_function()),
            "ProjectSet should have at least one table function."
        );

        let core = generic::ProjectSet { select_list, input };
        let base = PlanBase::new_logical_with_core(&core);

        LogicalProjectSet { base, core }
    }

    /// `create` will analyze select exprs with table functions and construct a plan.
    ///
    /// When table functions are used as arguments of a table function or a usual function, the
    /// arguments will be put at a lower `ProjectSet` while the call will be put at a higher
    /// `Project` or `ProjectSet`. The plan is like:
    ///
    /// ```text
    /// LogicalProjectSet/LogicalProject -> LogicalProjectSet -> input
    /// ```
    ///
    /// Otherwise it will be a simple `ProjectSet`.
    pub fn create(input: PlanRef, select_list: Vec<ExprImpl>) -> PlanRef {
        /// Rewrites a `FunctionCall` or `TableFunction` whose args contain table functions into one
        /// using `InputRef` as args.
        struct Rewriter {
            collected: Vec<TableFunction>,
            /// The nesting level of calls.
            ///
            /// f(x) has level 1 at x, and f(g(x)) has level 2 at x.
            level: usize,
            input_schema_len: usize,
        }

        impl ExprRewriter for Rewriter {
            fn rewrite_table_function(&mut self, table_func: TableFunction) -> ExprImpl {
                if self.level == 0 {
                    // Top-level table function doesn't need to be collected.
                    self.level += 1;

                    let TableFunction {
                        args,
                        return_type,
                        function_type,
                        udtf_catalog,
                    } = table_func;
                    let args = args
                        .into_iter()
                        .map(|expr| self.rewrite_expr(expr))
                        .collect();

                    self.level -= 1;
                    TableFunction {
                        args,
                        return_type,
                        function_type,
                        udtf_catalog,
                    }
                    .into()
                } else {
                    let input_ref = InputRef::new(
                        self.input_schema_len + self.collected.len(),
                        table_func.return_type(),
                    );
                    self.collected.push(table_func);
                    input_ref.into()
                }
            }

            fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
                self.level += 1;
                let (func_type, inputs, return_type) = func_call.decompose();
                let inputs = inputs
                    .into_iter()
                    .map(|expr| self.rewrite_expr(expr))
                    .collect();
                self.level -= 1;
                FunctionCall::new_unchecked(func_type, inputs, return_type).into()
            }
        }

        let mut rewriter = Rewriter {
            collected: vec![],
            level: 0,
            input_schema_len: input.schema().len(),
        };
        let select_list: Vec<_> = select_list
            .into_iter()
            .map(|e| rewriter.rewrite_expr(e))
            .collect();

        if rewriter.collected.is_empty() {
            LogicalProjectSet::new(input, select_list).into()
        } else {
            let mut inner_select_list: Vec<_> = input
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .map(|(i, ty)| InputRef::new(i, ty).into())
                .collect();
            inner_select_list.extend(rewriter.collected.into_iter().map(|tf| tf.into()));
            let inner = LogicalProjectSet::create(input, inner_select_list);

            /// Increase all the input ref in the outer select list, because the inner project set
            /// will output a hidden column at the beginning.
            struct IncInputRef {}
            impl ExprRewriter for IncInputRef {
                fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
                    InputRef::new(input_ref.index + 1, input_ref.data_type).into()
                }
            }
            let mut rewriter = IncInputRef {};
            let select_list: Vec<_> = select_list
                .into_iter()
                .map(|e| rewriter.rewrite_expr(e))
                .collect();

            if select_list.iter().any(|e| e.has_table_function()) {
                LogicalProjectSet::new(inner, select_list).into()
            } else {
                LogicalProject::new(inner, select_list).into()
            }
        }
    }

    pub fn select_list(&self) -> &Vec<ExprImpl> {
        &self.core.select_list
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let _verbose = self.base.ctx.is_explain_verbose();
        // TODO: add verbose display like Project

        self.core.fmt_with_name(f, name)
    }
}

impl PlanTreeNodeUnary for LogicalProjectSet {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.select_list().clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let select_list = self
            .select_list()
            .clone()
            .into_iter()
            .map(|item| input_col_change.rewrite_expr(item))
            .collect();
        let project_set = Self::new(input, select_list);
        // change the input columns index will not change the output column index
        let out_col_change = ColIndexMapping::identity(self.schema().len());
        (project_set, out_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalProjectSet}

impl fmt::Display for LogicalProjectSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalProjectSet")
    }
}

impl ColPrunable for LogicalProjectSet {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_col_num = self.input().schema().len();
        let mut input_required_appeared = FixedBitSet::with_capacity(input_col_num);

        // Record each InputRef's index.
        let mut input_ref_collector = CollectInputRef::with_capacity(input_col_num);
        required_cols.iter().filter(|&&i| i > 0).for_each(|&i| {
            if let ExprImpl::InputRef(ref input_ref) = self.select_list()[i - 1] {
                let input_idx = input_ref.index;
                input_required_appeared.put(input_idx);
            } else {
                input_ref_collector.visit_expr(&self.select_list()[i - 1]);
            }
        });
        let input_required_cols = {
            let mut tmp = FixedBitSet::from(input_ref_collector);
            tmp.union_with(&input_required_appeared);
            tmp
        };
        let input_required_cols = input_required_cols.ones().collect_vec();
        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        // Rewrite each InputRef with new index.
        let select_list = required_cols
            .iter()
            .filter(|&&id| id > 0)
            .map(|&id| mapping.rewrite_expr(self.select_list()[id - 1].clone()))
            .collect();

        // Reconstruct the LogicalProjectSet
        let new_node: PlanRef = LogicalProjectSet::new(new_input, select_list).into();
        if new_node.schema().len() == required_cols.len() {
            // current schema perfectly fit the required columns
            new_node
        } else {
            // some columns are not needed, or the order need to be adjusted.
            // so we did a projection to remove/reorder the columns.
            let mut new_output_cols = Vec::from(required_cols);
            if required_cols.first() != Some(&0) {
                new_output_cols.insert(0, 0);
            }
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = new_node.schema().len();
            LogicalProject::with_mapping(
                new_node,
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable for LogicalProjectSet {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl PredicatePushdown for LogicalProjectSet {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // TODO: predicate pushdown https://github.com/risingwavelabs/risingwave/issues/8591
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalProjectSet {
    fn to_batch(&self) -> Result<PlanRef> {
        let mut new_logical = self.core.clone();
        new_logical.input = self.input().to_batch()?;
        Ok(BatchProjectSet::new(new_logical).into())
    }
}

impl ToStream for LogicalProjectSet {
    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (project_set, out_col_change) =
            self.rewrite_with_input(input.clone(), input_col_change);

        // Add missing columns of input_pk into the select list.
        let input_pk = input.logical_pk();
        let i2o = self.core.i2o_col_mapping();
        let col_need_to_add = input_pk
            .iter()
            .cloned()
            .filter(|i| i2o.try_map(*i).is_none());
        let input_schema = input.schema();
        let select_list =
            project_set
                .select_list()
                .iter()
                .cloned()
                .chain(col_need_to_add.map(|idx| {
                    InputRef::new(idx, input_schema.fields[idx].data_type.clone()).into()
                }))
                .collect();
        let project_set = Self::new(input, select_list);
        // The added columns is at the end, so it will not change existing column indices.
        // But the target size of `out_col_change` should be the same as the length of the new
        // schema.
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::with_target_size(map, project_set.schema().len());
        Ok((project_set.into(), out_col_change))
    }

    // TODO: implement to_stream_with_dist_required like LogicalProject

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let new_input = self.input().to_stream(ctx)?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        Ok(StreamProjectSet::new(new_logical).into())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{ExprImpl, InputRef, TableFunction};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::property::FunctionalDependency;

    #[tokio::test]
    async fn fd_derivation_project_set() {
        // input: [v1, v2, v3]
        // FD: v2 --> v3
        // output: [projected_row_id, v3, v2, generate_series(v1, v2, v3)],
        // FD: v2 --> v3

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
            .add_functional_dependency_by_column_indices(&[1], &[2]);
        let project_set = LogicalProjectSet::new(
            values.into(),
            vec![
                ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
                ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                ExprImpl::TableFunction(Box::new(
                    TableFunction::new(
                        crate::expr::TableFunctionType::Generate,
                        vec![
                            ExprImpl::InputRef(Box::new(InputRef::new(0, DataType::Int32))),
                            ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                            ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
                        ],
                    )
                    .unwrap(),
                )),
            ],
        );
        let fd_set: HashSet<FunctionalDependency> = project_set
            .base
            .functional_dependency
            .into_dependencies()
            .into_iter()
            .collect();
        let expected_fd_set: HashSet<FunctionalDependency> =
            [FunctionalDependency::with_indices(4, &[2], &[1])]
                .into_iter()
                .collect();
        assert_eq!(fd_set, expected_fd_set);
    }
}
