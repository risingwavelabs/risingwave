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

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{SetExpr, SetOperator};

use super::statement::RewriteExprsRecursive;
use crate::binder::{BindContext, Binder, BoundQuery, BoundSelect, BoundValues};
use crate::expr::{CorrelatedId, Depth};

/// Part of a validated query, without order or limit clause. It may be composed of smaller
/// `BoundSetExpr`s via set operators (e.g. union).
#[derive(Debug, Clone)]
pub enum BoundSetExpr {
    Select(Box<BoundSelect>),
    Query(Box<BoundQuery>),
    Values(Box<BoundValues>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: BoundSetOperation,
        all: bool,
        left: Box<BoundSetExpr>,
        right: Box<BoundSetExpr>,
    },
}

impl RewriteExprsRecursive for BoundSetExpr {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        match self {
            BoundSetExpr::Select(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundSetExpr::Query(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundSetExpr::Values(inner) => inner.rewrite_exprs_recursive(rewriter),
            BoundSetExpr::SetOperation { left, right, .. } => {
                left.rewrite_exprs_recursive(rewriter);
                right.rewrite_exprs_recursive(rewriter);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum BoundSetOperation {
    Union,
    Except,
    Intersect,
}

impl From<SetOperator> for BoundSetOperation {
    fn from(value: SetOperator) -> Self {
        match value {
            SetOperator::Union => BoundSetOperation::Union,
            SetOperator::Intersect => BoundSetOperation::Intersect,
            SetOperator::Except => BoundSetOperation::Except,
        }
    }
}

impl BoundSetExpr {
    /// The schema returned by this [`BoundSetExpr`].

    pub fn schema(&self) -> &Schema {
        match self {
            BoundSetExpr::Select(s) => s.schema(),
            BoundSetExpr::Values(v) => v.schema(),
            BoundSetExpr::Query(q) => q.schema(),
            BoundSetExpr::SetOperation { left, .. } => left.schema(),
        }
    }

    pub fn is_correlated(&self, depth: Depth) -> bool {
        match self {
            BoundSetExpr::Select(s) => s.is_correlated(depth),
            BoundSetExpr::Values(v) => v.is_correlated(depth),
            BoundSetExpr::Query(q) => q.is_correlated(depth),
            BoundSetExpr::SetOperation { left, right, .. } => {
                left.is_correlated(depth) || right.is_correlated(depth)
            }
        }
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        match self {
            BoundSetExpr::Select(s) => {
                s.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            }
            BoundSetExpr::Values(v) => {
                v.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            }
            BoundSetExpr::Query(q) => {
                q.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            }
            BoundSetExpr::SetOperation { left, right, .. } => {
                let mut correlated_indices = vec![];
                correlated_indices.extend(
                    left.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices.extend(
                    right.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices
            }
        }
    }
}

impl Binder {
    pub(super) fn bind_set_expr(&mut self, set_expr: SetExpr) -> Result<BoundSetExpr> {
        match set_expr {
            SetExpr::Select(s) => Ok(BoundSetExpr::Select(Box::new(self.bind_select(*s)?))),
            SetExpr::Values(v) => Ok(BoundSetExpr::Values(Box::new(self.bind_values(v, None)?))),
            SetExpr::Query(q) => Ok(BoundSetExpr::Query(Box::new(self.bind_query(*q)?))),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                match op {
                    SetOperator::Union | SetOperator::Intersect | SetOperator::Except => {
                        let left = Box::new(self.bind_set_expr(*left)?);
                        // Reset context for right side, but keep `cte_to_relation`.
                        let new_context = std::mem::take(&mut self.context);
                        self.context.cte_to_relation = new_context.cte_to_relation.clone();
                        let right = Box::new(self.bind_set_expr(*right)?);

                        if left.schema().fields.len() != right.schema().fields.len() {
                            return Err(ErrorCode::InvalidInputSyntax(format!(
                                "each {} query must have the same number of columns",
                                op
                            ))
                            .into());
                        }

                        for (a, b) in left
                            .schema()
                            .fields
                            .iter()
                            .zip_eq_fast(right.schema().fields.iter())
                        {
                            if a.data_type != b.data_type {
                                return Err(ErrorCode::InvalidInputSyntax(format!(
                                    "{} types {} of column {} is different from types {} of column {}",
                                    op,
                                    a.data_type.prost_type_name().as_str_name(),
                                    a.name,
                                    b.data_type.prost_type_name().as_str_name(),
                                    b.name,
                                ))
                                    .into());
                            }
                        }

                        if all {
                            match op {
                                SetOperator::Union => {}
                                SetOperator::Intersect | SetOperator::Except => {
                                    return Err(ErrorCode::NotImplemented(
                                        format!("{} all", op),
                                        None.into(),
                                    )
                                    .into())
                                }
                            }
                        }

                        // Reset context for the set operation.
                        // Consider this case:
                        // select a from t2 union all select b from t2 order by a+1; should throw an
                        // error.
                        self.context = BindContext::default();
                        self.context.cte_to_relation = new_context.cte_to_relation;
                        Ok(BoundSetExpr::SetOperation {
                            op: op.into(),
                            all,
                            left,
                            right,
                        })
                    }
                }
            }
        }
    }
}
