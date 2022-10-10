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
//
use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    ColPrunable, LogicalJoin, LogicalProject, PlanBase, PlanRef, PlanTreeNodeBinary,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{CorrelatedId, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalApply` represents a correlated join, where the right side may refer to columns from the
/// left side.
#[derive(Debug, Clone)]
pub struct LogicalApply {
    pub base: PlanBase,
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,

    /// Id of the Apply operator.
    /// So correlated_input_ref can refer the Apply operator exactly by correlated_id.
    correlated_id: CorrelatedId,
    /// The indices of `CorrelatedInputRef`s in `right`.
    correlated_indices: Vec<usize>,
    /// If the subquery produces more than one result we have to report an error.
    max_one_row: bool,
}

impl fmt::Display for LogicalApply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("LogicalApply");

        builder.field("type", &format_args!("{:?}", &self.join_type));

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "on",
            &format_args!(
                "{}",
                ConditionDisplay {
                    condition: &self.on,
                    input_schema: &concat_schema
                }
            ),
        );

        builder.field("correlated_id", &self.correlated_id);

        if self.max_one_row {
            builder.field("max_one_row", &self.max_one_row);
        }

        builder.finish()
    }
}

impl LogicalApply {
    pub(crate) fn new(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
        max_one_row: bool,
    ) -> Self {
        let ctx = left.ctx();
        let out_column_num =
            LogicalJoin::out_column_num(left.schema().len(), right.schema().len(), join_type);
        let output_indices = (0..out_column_num).collect::<Vec<_>>();
        let schema =
            LogicalJoin::derive_schema(left.schema(), right.schema(), join_type, &output_indices);
        let pk_indices = LogicalJoin::derive_pk(
            left.schema().len(),
            right.schema().len(),
            left.logical_pk(),
            right.logical_pk(),
            join_type,
            &output_indices,
        );
        let functional_dependency = FunctionalDependencySet::with_key(schema.len(), &pk_indices);
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        LogicalApply {
            base,
            left,
            right,
            on,
            join_type,
            correlated_id,
            correlated_indices,
            max_one_row,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
        max_one_row: bool,
    ) -> PlanRef {
        Self::new(
            left,
            right,
            join_type,
            on,
            correlated_id,
            correlated_indices,
            max_one_row,
        )
        .into()
    }

    /// Get the join type of the logical apply.
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn decompose(
        self,
    ) -> (
        PlanRef,
        PlanRef,
        Condition,
        JoinType,
        CorrelatedId,
        Vec<usize>,
        bool,
    ) {
        (
            self.left,
            self.right,
            self.on,
            self.join_type,
            self.correlated_id,
            self.correlated_indices,
            self.max_one_row,
        )
    }

    pub fn correlated_id(&self) -> CorrelatedId {
        self.correlated_id
    }

    pub fn correlated_indices(&self) -> Vec<usize> {
        self.correlated_indices.to_owned()
    }

    pub fn max_one_row(&self) -> bool {
        self.max_one_row
    }

    /// Translate Apply.
    ///
    /// Used to convert other kinds of Apply to cross Apply.
    pub fn translate_apply(self, new_apply_left: PlanRef, eq_predicates: Vec<ExprImpl>) -> PlanRef {
        let (
            apply_left,
            apply_right,
            on,
            apply_type,
            correlated_id,
            correlated_indices,
            max_one_row,
        ) = self.decompose();
        let apply_left_len = apply_left.schema().len();
        let correlated_indices_len = correlated_indices.len();

        let new_apply = LogicalApply::create(
            new_apply_left,
            apply_right,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            max_one_row,
        );

        let on = Self::rewrite_on(on, correlated_indices_len, apply_left_len).and(Condition {
            conjunctions: eq_predicates,
        });
        let new_join = LogicalJoin::new(apply_left, new_apply, apply_type, on);

        if new_join.join_type() != JoinType::LeftSemi {
            // `new_join`'s schema is different from original apply's schema, so `LogicalProject` is
            // used to ensure they are the same.
            let mut exprs: Vec<ExprImpl> = vec![];
            new_join
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .for_each(|(index, data_type)| {
                    if index < apply_left_len || index >= apply_left_len + correlated_indices_len {
                        exprs.push(InputRef::new(index, data_type).into());
                    }
                });
            LogicalProject::create(new_join.into(), exprs)
        } else {
            new_join.into()
        }
    }

    fn rewrite_on(on: Condition, offset: usize, apply_left_len: usize) -> Condition {
        struct Rewriter {
            offset: usize,
            apply_left_len: usize,
        }
        impl ExprRewriter for Rewriter {
            fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
                let index = input_ref.index();
                if index >= self.apply_left_len {
                    InputRef::new(index + self.offset, input_ref.return_type()).into()
                } else {
                    input_ref.into()
                }
            }
        }
        let mut rewriter = Rewriter {
            offset,
            apply_left_len,
        };
        on.rewrite_expr(&mut rewriter)
    }
}

impl PlanTreeNodeBinary for LogicalApply {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            left,
            right,
            self.join_type,
            self.on.clone(),
            self.correlated_id,
            self.correlated_indices.clone(),
            self.max_one_row,
        )
    }
}

impl_plan_tree_node_for_binary! { LogicalApply }

impl ColPrunable for LogicalApply {
    fn prune_col(&self, _: &[usize]) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl PredicatePushdown for LogicalApply {
    fn predicate_pushdown(&self, _predicate: Condition) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl ToBatch for LogicalApply {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_string(),
        )))
    }
}

impl ToStream for LogicalApply {
    fn to_stream(&self) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_string(),
        )))
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_string(),
        )))
    }
}
