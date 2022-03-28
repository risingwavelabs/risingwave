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

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::plan::JoinType;

use crate::binder::BoundSelect;
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Subquery, SubqueryKind,
};
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalApply, LogicalProject, LogicalValues, PlanAggCall, PlanRef,
};
use crate::planner::Planner;
impl Planner {
    pub(super) fn plan_select(
        &mut self,
        BoundSelect {
            from,
            where_clause,
            mut select_items,
            group_by,
            aliases,
            ..
        }: BoundSelect,
    ) -> Result<PlanRef> {
        // Plan the FROM clause.
        let mut root = match from {
            None => self.create_dummy_values(),
            Some(t) => self.plan_relation(t)?,
        };
        // Plan the WHERE clause.
        root = match where_clause {
            None => root,
            Some(mut predicate) => {
                if predicate.has_subquery() {
                    (root, predicate) = self.substitute_subqueries(root, predicate)?;
                }
                LogicalFilter::create(root, predicate)?
            }
        };
        // Plan the SELECT clause.
        // TODO: select-agg, group-by, having can also contain subquery exprs.
        let has_agg_call = select_items.iter().any(|expr| expr.has_agg_call());
        if !group_by.is_empty() || has_agg_call {
            LogicalAgg::create(select_items, aliases, group_by, root)
        } else {
            if select_items.iter().any(|e| e.has_subquery()) {
                (root, select_items) = self.substitute_subqueries_vec(root, select_items)?;
            }
            Ok(LogicalProject::create(root, select_items, aliases))
        }
    }

    /// Helper to create a dummy node as child of [`LogicalProject`].
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[]])`.
    fn create_dummy_values(&self) -> PlanRef {
        LogicalValues::create(vec![vec![]], Schema::default(), self.ctx.clone())
    }

    /// Helper to create an `EXISTS` boolean operator with the given `input`.
    /// It is represented by `Project([$0 >= 1]) - Agg(count(*)) - input`
    fn create_exists(&self, input: PlanRef) -> Result<PlanRef> {
        let count_star =
            LogicalAgg::new(vec![PlanAggCall::count_star()], vec![None], vec![], input);
        let ge = FunctionCall::new(
            ExprType::GreaterThanOrEqual,
            vec![
                InputRef::new(0, DataType::Int64).into(),
                ExprImpl::literal_int(1),
            ],
        )
        .unwrap();
        Ok(LogicalProject::create(
            count_star.into(),
            vec![ge.into()],
            vec![None],
        ))
    }

    /// A wrapper around [`substitute_subqueries_vec`]
    fn substitute_subqueries(
        &mut self,
        mut root: PlanRef,
        expr: ExprImpl,
    ) -> Result<(PlanRef, ExprImpl)> {
        let mut exprs = vec![expr];
        (root, exprs) = self.substitute_subqueries_vec(root, exprs)?;
        Ok((root, exprs.into_iter().next().unwrap()))
    }

    /// Substitues all [`Subquery`] in `exprs`.
    ///
    /// Each time a [`Subquery`] is found, it is replaced by a new [`InputRef`]. And `root` is
    /// replaced by a new [`LogicalApply`] node, whose left side is `root` and right side is the
    /// planned subquery.
    ///
    /// The [`InputRef`]s' indexes start from `root.schema().len()`,
    /// which means they are additional columns beyond the original `root`.
    fn substitute_subqueries_vec(
        &mut self,
        mut root: PlanRef,
        mut exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        struct SubstituteSubQueries {
            input_col_num: usize,
            subqueries: Vec<Subquery>,
        }

        impl ExprRewriter for SubstituteSubQueries {
            fn rewrite_subquery(&mut self, subquery: Subquery) -> ExprImpl {
                let input_ref = InputRef::new(self.input_col_num, subquery.return_type()).into();
                self.subqueries.push(subquery);
                self.input_col_num += 1;
                input_ref
            }
        }

        let mut rewriter = SubstituteSubQueries {
            input_col_num: root.schema().len(),
            subqueries: vec![],
        };
        exprs = exprs
            .into_iter()
            .map(|e| rewriter.rewrite_expr(e))
            .collect();

        for subquery in rewriter.subqueries {
            let mut right = self.plan_query(subquery.query)?.as_subplan();

            match subquery.kind {
                SubqueryKind::Scalar => {}
                SubqueryKind::Existential => {
                    right = self.create_exists(right)?;
                }
                SubqueryKind::SetComparision => {
                    return Err(
                        ErrorCode::NotImplementedError(format!("{:?}", subquery.kind)).into(),
                    )
                }
            }

            root = LogicalApply::create(root, right, JoinType::LeftOuter);
        }
        Ok((root, exprs))
    }
}
