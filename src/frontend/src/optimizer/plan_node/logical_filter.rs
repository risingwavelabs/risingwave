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
use risingwave_common::bail;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::generic::{self, GenericPlanNode};
use super::{
    ColPrunable, CollectInputRef, ExprRewritable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{assert_input_ref, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{
    BatchFilter, ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext,
    StreamFilter, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalFilter` iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalFilter {
    pub base: PlanBase,
    core: generic::Filter<PlanRef>,
}

impl LogicalFilter {
    pub fn new(input: PlanRef, predicate: Condition) -> Self {
        let ctx = input.ctx();
        for cond in &predicate.conjunctions {
            assert_input_ref!(cond, input.schema().fields().len());
        }
        let core = generic::Filter { predicate, input };
        let base = PlanBase::new_logical_with_core(&core);
        LogicalFilter { base, core }
    }

    /// Create a `LogicalFilter` unless the predicate is always true
    pub fn create(input: PlanRef, predicate: Condition) -> PlanRef {
        if predicate.always_true() {
            input
        } else {
            LogicalFilter::new(input, predicate).into()
        }
    }

    /// Create a `LogicalFilter` to filter the rows with all keys are null.
    pub fn filter_if_keys_all_null(input: PlanRef, key: &[usize]) -> PlanRef {
        let schema = input.schema();
        let cond = key.iter().fold(ExprImpl::literal_bool(false), |expr, i| {
            ExprImpl::FunctionCall(
                FunctionCall::new_unchecked(
                    ExprType::Or,
                    vec![
                        expr,
                        FunctionCall::new_unchecked(
                            ExprType::IsNotNull,
                            vec![InputRef::new(*i, schema.fields()[*i].data_type.clone()).into()],
                            DataType::Boolean,
                        )
                        .into(),
                    ],
                    DataType::Boolean,
                )
                .into(),
            )
        });
        LogicalFilter::create_with_expr(input, cond)
    }

    pub fn create_with_expr(input: PlanRef, predicate: ExprImpl) -> PlanRef {
        let predicate = Condition::with_expr(predicate);
        Self::new(input, predicate).into()
    }

    /// Get the predicate of the logical join.
    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let input = self.input();
        let input_schema = input.schema();
        write!(
            f,
            "{} {{ predicate: {} }}",
            name,
            ConditionDisplay {
                condition: self.predicate(),
                input_schema
            }
        )
    }
}

impl PlanTreeNodeUnary for LogicalFilter {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.predicate().clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        mut input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let predicate = self.predicate().clone().rewrite_expr(&mut input_col_change);
        (Self::new(input, predicate), input_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalFilter}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalFilter")
    }
}

impl ColPrunable for LogicalFilter {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let required_cols_bitset = FixedBitSet::from_iter(required_cols.iter().copied());

        let mut visitor = CollectInputRef::with_capacity(self.input().schema().len());
        self.predicate().visit_expr(&mut visitor);
        let predicate_required_cols: FixedBitSet = visitor.into();

        let mut predicate = self.predicate().clone();
        let input_required_cols = {
            let mut tmp = predicate_required_cols;
            tmp.union_with(&required_cols_bitset);
            tmp.ones().collect_vec()
        };
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        predicate = predicate.rewrite_expr(&mut mapping);

        let filter =
            LogicalFilter::new(self.input().prune_col(&input_required_cols, ctx), predicate);
        if input_required_cols == required_cols {
            filter.into()
        } else {
            // Given that `LogicalFilter` always has same schema of its input, if predicate's
            // `InputRef`s are not subset of the requirement of downstream operator, a
            // `LogicalProject` node should be added.
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = filter.schema().len();
            LogicalProject::with_mapping(
                filter.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable for LogicalFilter {
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

impl PredicatePushdown for LogicalFilter {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let predicate = predicate.and(self.predicate().clone());
        self.input().predicate_pushdown(predicate, ctx)
    }
}

impl ToBatch for LogicalFilter {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchFilter::new(new_logical).into())
    }
}

impl ToStream for LogicalFilter {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let new_input = self.input().to_stream(ctx)?;

        let predicate = self.predicate();
        let has_now = predicate
            .conjunctions
            .iter()
            .any(|cond| cond.count_nows() > 0);
        if has_now {
            if predicate
                .conjunctions
                .iter()
                .any(|expr| expr.count_nows() > 0 && expr.as_now_comparison_cond().is_none())
            {
                bail!(
                    "Conditions containing now must be of the form `input_expr cmp now() [+- const_expr]` or \
                    `now() [+- const_expr] cmp input_expr`, where `input_expr` references a column \
                    and contains no `now()`."
                );
            }
            bail!(
                "All `now()` exprs were valid, but the condition must have at least one now expr as a lower bound."
            );
        }
        let new_logical = self.clone_with_input(new_input);
        Ok(StreamFilter::new(new_logical).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (filter, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((filter.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::property::FunctionalDependency;

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [2] will result in
    /// ```text
    /// Project(input_ref(1))
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_filter() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(None, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let filter: PlanRef =
            LogicalFilter::new(values.into(), Condition::with_expr(predicate)).into();

        // Perform the prune
        let required_cols = vec![2];
        let plan = filter.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(filter.clone()),
        );

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let filter = project.input();
        let filter = filter.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[1]);
        assert_eq!(filter.schema().fields()[1], fields[2]);
        assert_eq!(filter.id().0, 4);

        let expr: ExprImpl = filter.predicate().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        let values = filter.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[1]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<null)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1, 0] will result in
    /// ```text
    /// Project(input_ref(1), input_ref(0))
    ///   Filter(cond: input_ref(1)<null)
    ///     TableScan(v1, v2)
    /// ```
    async fn test_prune_filter_with_order_required() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(None, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let filter: PlanRef =
            LogicalFilter::new(values.into(), Condition::with_expr(predicate)).into();

        // Perform the prune
        let required_cols = vec![1, 0];
        let plan = filter.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(filter.clone()),
        );

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        assert_eq_input_ref!(&project.exprs()[1], 0);

        let filter = project.input();
        let filter = filter.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[0]);
        assert_eq!(filter.schema().fields()[1], fields[1]);

        let expr: ExprImpl = filter.predicate().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 1);
        let values = filter.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[1]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1, 2] will result in
    /// ```text
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_filter_no_project() {
        let ctx = OptimizerContext::mock().await;
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );

        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                ],
            )
            .unwrap(),
        ));
        let filter: PlanRef =
            LogicalFilter::new(values.into(), Condition::with_expr(predicate)).into();

        // Perform the prune
        let required_cols = vec![1, 2];
        let plan = filter.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(filter.clone()),
        );

        // Check the result
        let filter = plan.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[1]);
        assert_eq!(filter.schema().fields()[1], fields[2]);
        let expr: ExprImpl = filter.predicate().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);

        let values = filter.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[1]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }

    #[tokio::test]
    async fn fd_derivation_filter() {
        // input: [v1, v2, v3, v4]
        // FD: v4 --> { v2, v3 }
        // Condition: v1 = 0 AND v2 = v3
        // output: [v1, v2, v3, v4],
        // FD: v4 --> { v2, v3 }, {} --> v1, v2 --> v3, v3 --> v2
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
            Field::with_name(DataType::Int32, "v4"),
        ];
        let mut values = LogicalValues::new(vec![], Schema { fields }, ctx);
        // 3 --> 1, 2
        values
            .base
            .functional_dependency
            .add_functional_dependency_by_column_indices(&[3], &[1, 2]);
        // v1 = 0 AND v2 = v3
        let predicate = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::And,
                vec![
                    ExprImpl::FunctionCall(Box::new(
                        FunctionCall::new(
                            Type::Equal,
                            vec![
                                ExprImpl::InputRef(Box::new(InputRef::new(0, DataType::Int32))),
                                ExprImpl::Literal(Box::new(Literal::new(
                                    Some(ScalarImpl::Int32(1)),
                                    DataType::Int32,
                                ))),
                            ],
                        )
                        .unwrap(),
                    )),
                    ExprImpl::FunctionCall(Box::new(
                        FunctionCall::new(
                            Type::Equal,
                            vec![
                                ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                                ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
                            ],
                        )
                        .unwrap(),
                    )),
                ],
            )
            .unwrap(),
        ));
        let filter = LogicalFilter::create_with_expr(values.into(), predicate);
        let fd_set: HashSet<_> = filter
            .functional_dependency()
            .as_dependencies()
            .iter()
            .cloned()
            .collect();
        let expected_fd_set: HashSet<_> = [
            FunctionalDependency::with_indices(4, &[3], &[1, 2]),
            FunctionalDependency::with_indices(4, &[], &[0]),
            FunctionalDependency::with_indices(4, &[1], &[2]),
            FunctionalDependency::with_indices(4, &[2], &[1]),
        ]
        .into_iter()
        .collect();
        assert_eq!(fd_set, expected_fd_set);
    }
}
