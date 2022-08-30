// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use crate::binder::BoundSelect;
use crate::expr::{
    CorrelatedId, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Subquery,
    SubqueryKind,
};
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalApply, LogicalJoin, LogicalProject, LogicalProjectSet, LogicalValues,
    PlanAggCall, PlanRef,
};
use crate::planner::Planner;
use crate::utils::Condition;
impl Planner {
    pub(super) fn plan_select(
        &mut self,
        BoundSelect {
            from,
            where_clause,
            mut select_items,
            group_by,
            mut having,
            distinct,
            ..
        }: BoundSelect,
        extra_order_exprs: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        // Append expressions in ORDER BY.
        if distinct && !extra_order_exprs.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
            )
            .into());
        }
        select_items.extend(extra_order_exprs);

        // Plan the FROM clause.
        let mut root = match from {
            None => self.create_dummy_values(),
            Some(t) => self.plan_relation(t)?,
        };
        // Plan the WHERE clause.
        if let Some(where_clause) = where_clause {
            root = self.plan_where(root, where_clause)?;
        }
        // Plan the SELECT clause.
        // TODO: select-agg, group-by, having can also contain subquery exprs.
        let has_agg_call = select_items.iter().any(|expr| expr.has_agg_call());
        if !group_by.is_empty() || having.is_some() || has_agg_call {
            (root, select_items, having) =
                LogicalAgg::create(select_items, group_by, having, root)?;
        }

        if let Some(having) = having {
            root = self.plan_where(root, having)?;
        }

        if select_items.iter().any(|e| e.has_subquery()) {
            (root, select_items) = self.substitute_subqueries(root, select_items)?;
        }
        if select_items.iter().any(|e| e.has_window_function()) {
            return Err(ErrorCode::NotImplemented(
                "plan LogicalWindowAgg".to_string(),
                4847.into(),
            )
            .into());
        }

        if select_items.iter().any(|e| e.has_table_function()) {
            root = LogicalProjectSet::create(root, select_items)
        } else {
            root = LogicalProject::create(root, select_items);
        }

        if distinct {
            let group_key = (0..root.schema().fields().len()).collect();
            root = LogicalAgg::new(vec![], group_key, root).into();
        }

        Ok(root)
    }

    /// Helper to create a dummy node as child of [`LogicalProject`].
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[]])`.
    fn create_dummy_values(&self) -> PlanRef {
        LogicalValues::create(vec![vec![]], Schema::default(), self.ctx.clone())
    }

    /// Helper to create an `EXISTS` boolean operator with the given `input`.
    /// It is represented by `Project([$0 >= 1]) -> Agg(count(*)) -> input`
    fn create_exists(&self, input: PlanRef) -> Result<PlanRef> {
        let count_star = LogicalAgg::new(vec![PlanAggCall::count_star()], vec![], input);
        let ge = FunctionCall::new(
            ExprType::GreaterThanOrEqual,
            vec![
                InputRef::new(0, DataType::Int64).into(),
                ExprImpl::literal_int(1),
            ],
        )
        .unwrap();
        Ok(LogicalProject::create(count_star.into(), vec![ge.into()]))
    }

    /// For `(NOT) EXISTS subquery` or `(NOT) IN subquery`, we can plan it as
    /// `LeftSemi/LeftAnti` [`LogicalApply`] (correlated) or [`LogicalJoin`].
    ///
    /// For other subqueries, we plan it as `LeftOuter` [`LogicalApply`] (correlated) or
    /// [`LogicalJoin`] using [`Self::substitute_subqueries`].
    fn plan_where(&mut self, mut input: PlanRef, where_clause: ExprImpl) -> Result<PlanRef> {
        if !where_clause.has_subquery() {
            return Ok(LogicalFilter::create_with_expr(input, where_clause));
        }
        let (subquery_conjunctions, not_subquery_conjunctions, others) =
            Condition::with_expr(where_clause)
                .group_by::<_, 3>(|expr| match expr {
                    ExprImpl::Subquery(_) => 0,
                    ExprImpl::FunctionCall(func_call)
                        if func_call.get_expr_type() == ExprType::Not
                            && matches!(func_call.inputs()[0], ExprImpl::Subquery(_)) =>
                    {
                        1
                    }
                    _ => 2,
                })
                .into_iter()
                .next_tuple()
                .unwrap();

        // EXISTS and IN in WHERE.
        for expr in subquery_conjunctions {
            self.handle_exists_and_in(expr, false, &mut input)?;
        }

        // NOT EXISTS and NOT IN in WHERE.
        for expr in not_subquery_conjunctions {
            let not = expr.into_function_call().unwrap();
            let (_, expr) = not.decompose_as_unary();
            self.handle_exists_and_in(expr, true, &mut input)?;
        }

        if others.always_true() {
            Ok(input)
        } else {
            let (input, others) = self.substitute_subqueries(input, others.conjunctions)?;
            Ok(LogicalFilter::create(
                input,
                Condition {
                    conjunctions: others,
                },
            ))
        }
    }

    /// Handle (NOT) EXISTS and (NOT) IN in WHERE clause.
    ///
    /// We will use a = b to replace a in (select b from ....) for (NOT) IN thus avoiding adding a
    /// `LogicalFilter` on `LogicalApply`.
    fn handle_exists_and_in(
        &mut self,
        expr: ExprImpl,
        negated: bool,
        input: &mut PlanRef,
    ) -> Result<()> {
        let join_type = if negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let correlated_id = self.ctx.next_correlated_id();
        let mut subquery = expr.into_subquery().unwrap();
        let correlated_indices =
            subquery.collect_correlated_indices_by_depth_and_assign_id(0, correlated_id);
        let output_column_type = subquery.query.data_types()[0].clone();
        let right_plan = self.plan_query(subquery.query)?.into_subplan();
        let on = match subquery.kind {
            SubqueryKind::Existential => ExprImpl::literal_bool(true),
            SubqueryKind::In(left_expr) => {
                let right_expr = InputRef::new(input.schema().len(), output_column_type);
                FunctionCall::new(ExprType::Equal, vec![left_expr, right_expr.into()])?.into()
            }
            kind => {
                return Err(ErrorCode::NotImplemented(
                    format!("Not supported subquery kind: {:?}", kind),
                    1343.into(),
                )
                .into())
            }
        };
        *input = Self::create_join(
            correlated_id,
            correlated_indices,
            input.clone(),
            right_plan,
            on,
            join_type,
        );
        Ok(())
    }

    /// Substitutes all [`Subquery`] in `exprs`.
    ///
    /// Each time a [`Subquery`] is found, it is replaced by a new [`InputRef`]. And `root` is
    /// replaced by a new `LeftOuter` [`LogicalApply`] (correlated) or [`LogicalJoin`]
    /// (uncorrelated) node, whose left side is `root` and right side is the planned subquery.
    ///
    /// The [`InputRef`]s' indexes start from `root.schema().len()`,
    /// which means they are additional columns beyond the original `root`.
    fn substitute_subqueries(
        &mut self,
        mut root: PlanRef,
        mut exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        struct SubstituteSubQueries {
            input_col_num: usize,
            subqueries: Vec<Subquery>,
            correlated_indices_collection: Vec<Vec<usize>>,
            correlated_id: CorrelatedId,
        }

        // TODO: consider the multi-subquery case for normal predicate.
        impl ExprRewriter for SubstituteSubQueries {
            fn rewrite_subquery(&mut self, mut subquery: Subquery) -> ExprImpl {
                let input_ref = InputRef::new(self.input_col_num, subquery.return_type()).into();
                self.input_col_num += 1;
                self.correlated_indices_collection.push(
                    subquery
                        .collect_correlated_indices_by_depth_and_assign_id(0, self.correlated_id),
                );
                self.subqueries.push(subquery);
                input_ref
            }
        }

        let correlated_id = self.ctx.next_correlated_id();
        let mut rewriter = SubstituteSubQueries {
            input_col_num: root.schema().len(),
            subqueries: vec![],
            correlated_indices_collection: vec![],
            correlated_id,
        };
        exprs = exprs
            .into_iter()
            .map(|e| rewriter.rewrite_expr(e))
            .collect();

        for (subquery, correlated_indices) in rewriter
            .subqueries
            .into_iter()
            .zip_eq(rewriter.correlated_indices_collection)
        {
            let mut right = self.plan_query(subquery.query)?.into_subplan();

            match subquery.kind {
                SubqueryKind::Scalar => {}
                SubqueryKind::Existential => {
                    right = self.create_exists(right)?;
                }
                _ => {
                    return Err(ErrorCode::NotImplemented(
                        format!("{:?}", subquery.kind),
                        1343.into(),
                    )
                    .into())
                }
            }

            root = Self::create_join(
                correlated_id,
                correlated_indices,
                root,
                right,
                ExprImpl::literal_bool(true),
                JoinType::LeftOuter,
            );
        }
        Ok((root, exprs))
    }

    fn create_join(
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
        left: PlanRef,
        right: PlanRef,
        on: ExprImpl,
        join_type: JoinType,
    ) -> PlanRef {
        if !correlated_indices.is_empty() {
            LogicalApply::create(
                left,
                right,
                join_type,
                Condition::with_expr(on),
                correlated_id,
                correlated_indices,
            )
        } else {
            LogicalJoin::create(left, right, join_type, on)
        }
    }
}
