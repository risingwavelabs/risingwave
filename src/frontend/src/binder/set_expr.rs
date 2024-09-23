// Copyright 2024 RisingWave Labs
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

use std::borrow::Cow;
use std::collections::HashMap;

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Schema;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{Corresponding, SetExpr, SetOperator};

use super::statement::RewriteExprsRecursive;
use super::UNNAMED_COLUMN;
use crate::binder::{BindContext, Binder, BoundQuery, BoundSelect, BoundValues};
use crate::error::{ErrorCode, Result};
use crate::expr::{align_types, CorrelatedId, Depth};

/// Part of a validated query, without order or limit clause. It may be composed of smaller
/// `BoundSetExpr`(s) via set operators (e.g., union).
#[derive(Debug, Clone)]
pub enum BoundSetExpr {
    Select(Box<BoundSelect>),
    Query(Box<BoundQuery>),
    Values(Box<BoundValues>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: BoundSetOperation,
        all: bool,
        // Corresponding columns of the left and right side.
        corresponding_col_indices: Option<(ColIndexMapping, ColIndexMapping)>,
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

    pub fn schema(&self) -> Cow<'_, Schema> {
        match self {
            BoundSetExpr::Select(s) => Cow::Borrowed(s.schema()),
            BoundSetExpr::Values(v) => Cow::Borrowed(v.schema()),
            BoundSetExpr::Query(q) => q.schema(),
            BoundSetExpr::SetOperation {
                left,
                corresponding_col_indices,
                ..
            } => {
                if let Some((mapping_l, _)) = corresponding_col_indices {
                    let mut schema = vec![None; mapping_l.target_size()];
                    for (src, tar) in mapping_l.mapping_pairs() {
                        assert_eq!(schema[tar], None);
                        schema[tar] = Some(left.schema().fields[src].clone());
                    }
                    Cow::Owned(Schema::new(
                        schema.into_iter().map(|x| x.unwrap()).collect(),
                    ))
                } else {
                    left.schema()
                }
            }
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
    /// note: `align_schema` only works when the `left` and `right`
    /// are both select expression(s).
    pub(crate) fn align_schema(
        mut left: &mut BoundSetExpr,
        mut right: &mut BoundSetExpr,
        op: SetOperator,
    ) -> Result<()> {
        if left.schema().fields.len() != right.schema().fields.len() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "each {} query must have the same number of columns",
                op
            ))
            .into());
        }

        // handle type alignment for select union select
        // e.g., select 1 UNION ALL select NULL
        if let (BoundSetExpr::Select(l_select), BoundSetExpr::Select(r_select)) =
            (&mut left, &mut right)
        {
            for (i, (l, r)) in l_select
                .select_items
                .iter_mut()
                .zip_eq_fast(r_select.select_items.iter_mut())
                .enumerate()
            {
                let Ok(column_type) = align_types(vec![l, r].into_iter()) else {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "{} types {} and {} cannot be matched. Columns' name are `{}` and `{}`.",
                        op,
                        l_select.schema.fields[i].data_type,
                        r_select.schema.fields[i].data_type,
                        l_select.schema.fields[i].name,
                        r_select.schema.fields[i].name,
                    ))
                    .into());
                };
                l_select.schema.fields[i].data_type = column_type.clone();
                r_select.schema.fields[i].data_type = column_type;
            }
        }

        Self::validate(left, right, op)
    }

    /// validate the schema, should be called after aligning.
    pub(crate) fn validate(
        left: &BoundSetExpr,
        right: &BoundSetExpr,
        op: SetOperator,
    ) -> Result<()> {
        for (a, b) in left
            .schema()
            .fields
            .iter()
            .zip_eq_fast(right.schema().fields.iter())
        {
            if a.data_type != b.data_type {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "{} types {} and {} cannot be matched. Columns' name are {} and {}.",
                    op,
                    a.data_type.prost_type_name().as_str_name(),
                    b.data_type.prost_type_name().as_str_name(),
                    a.name,
                    b.name,
                ))
                .into());
            }
        }
        Ok(())
    }

    /// Check the corresponding specification of the set operation.
    /// Returns the corresponding column index of the left and right side.
    fn corresponding(
        &self,
        left: &BoundSetExpr,
        right: &BoundSetExpr,
        corresponding: Corresponding,
        op: &SetOperator,
    ) -> Result<(ColIndexMapping, ColIndexMapping)> {
        let check_duplicate_name = |set_expr: &BoundSetExpr| {
            let mut name2idx = HashMap::new();
            for (idx, field) in set_expr.schema().fields.iter().enumerate() {
                if name2idx.insert(field.name.clone(), idx).is_some() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "Duplicated column name `{}` in a column list of the query in a {} operation. Column list of the query: ({}).",
                        field.name, op, set_expr.schema().formatted_col_names(),
                    )));
                }
            }
            Ok(name2idx)
        };

        // Within the columns of both side, the same <column name> shall not
        // be specified more than once.
        let name2idx_l = check_duplicate_name(left)?;
        let name2idx_r = check_duplicate_name(right)?;

        let mut corresponding_col_idx_l = vec![];
        let mut corresponding_col_idx_r = vec![];

        if let Some(column_list) = corresponding.column_list() {
            // The select list of the corresponding set operation should be in the order of <corresponding column list>
            for column in column_list {
                let col_name = column.real_value();
                if let Some(idx_l) = name2idx_l.get(&col_name)
                    && let Some(idx_r) = name2idx_l.get(&col_name)
                {
                    corresponding_col_idx_l.push(*idx_l);
                    corresponding_col_idx_r.push(*idx_r);
                } else {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "Column name `{}` in CORRESPONDING BY is not found in a side of the {} operation. \
                        It shall be included in both sides.",
                        col_name,
                        op,
                    )).into());
                }
            }
        } else {
            // The select list of the corresponding set operation should be
            // in the order that appears in the <column name>s of the left side.
            for field in &left.schema().fields {
                let col_name = &field.name;
                if col_name != UNNAMED_COLUMN
                    && let Some(idx_l) = name2idx_l.get(col_name)
                    && let Some(idx_r) = name2idx_r.get(col_name)
                {
                    corresponding_col_idx_l.push(*idx_l);
                    corresponding_col_idx_r.push(*idx_r);
                }
            }

            if corresponding_col_idx_l.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(
                    format!(
                        "When CORRESPONDING is specified, at least one column of the left side \
                        shall have a column name that is the column name of some column of the right side in a {} operation. \
                        Left side query column list: ({}). \
                        Right side query column list: ({}).",
                        op,
                        left.schema().formatted_col_names(),
                        right.schema().formatted_col_names(),
                    )
                )
                .into());
            }
        }

        let corresponding_mapping_l =
            ColIndexMapping::with_remaining_columns(&corresponding_col_idx_l, left.schema().len());
        let corresponding_mapping_r =
            ColIndexMapping::with_remaining_columns(&corresponding_col_idx_r, right.schema().len());

        Ok((corresponding_mapping_l, corresponding_mapping_r))
    }

    pub(super) fn bind_set_expr(&mut self, set_expr: SetExpr) -> Result<BoundSetExpr> {
        match set_expr {
            SetExpr::Select(s) => Ok(BoundSetExpr::Select(Box::new(self.bind_select(*s)?))),
            SetExpr::Values(v) => Ok(BoundSetExpr::Values(Box::new(self.bind_values(v, None)?))),
            SetExpr::Query(q) => Ok(BoundSetExpr::Query(Box::new(self.bind_query(*q)?))),
            SetExpr::SetOperation {
                op,
                all,
                corresponding,
                left,
                right,
            } => {
                match op.clone() {
                    SetOperator::Union | SetOperator::Intersect | SetOperator::Except => {
                        let mut left = self.bind_set_expr(*left)?;
                        // Reset context for right side, but keep `cte_to_relation`.
                        let new_context = std::mem::take(&mut self.context);
                        self.context
                            .cte_to_relation
                            .clone_from(&new_context.cte_to_relation);
                        let mut right = self.bind_set_expr(*right)?;

                        let corresponding_col_indices = if corresponding.is_corresponding() {
                            Some(Self::corresponding(
                                self,
                                &left,
                                &right,
                                corresponding,
                                &op,
                            )?)
                            // TODO: Align schema
                        } else {
                            Self::align_schema(&mut left, &mut right, op.clone())?;
                            None
                        };

                        if all {
                            match op {
                                SetOperator::Union => {}
                                SetOperator::Intersect | SetOperator::Except => {
                                    bail_not_implemented!("{} all", op);
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
                            corresponding_col_indices,
                            left: Box::new(left),
                            right: Box::new(right),
                        })
                    }
                }
            }
        }
    }
}
