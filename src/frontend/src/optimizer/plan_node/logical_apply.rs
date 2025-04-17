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

//
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Schema;
use risingwave_pb::plan_common::JoinType;

use super::generic::{
    self, GenericPlanNode, GenericPlanRef, push_down_into_join, push_down_join_condition,
};
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, Logical, LogicalJoin, LogicalProject, PlanBase, PlanRef, PlanTreeNodeBinary,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{CorrelatedId, Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, ExprRewritable, LogicalFilter, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalApply` represents a correlated join, where the right side may refer to columns from the
/// left side.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalApply {
    pub base: PlanBase<Logical>,
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,

    /// Id of the Apply operator.
    /// So `correlated_input_ref` can refer the Apply operator exactly by `correlated_id`.
    correlated_id: CorrelatedId,
    /// The indices of `CorrelatedInputRef`s in `right`.
    correlated_indices: Vec<usize>,
    /// Whether we require the subquery to produce at most one row. If `true`, we have to report an
    /// error if the subquery produces more than one row.
    max_one_row: bool,

    /// An apply has been translated by `translate_apply()`, so we should not translate it in `translate_apply_rule` again.
    /// This flag is used to avoid infinite loop in General Unnesting(Translate Apply), since we use a top-down apply order instead of bottom-up to improve the multi-scalar subqueries optimization time.
    translated: bool,
}

impl Distill for LogicalApply {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut vec = Vec::with_capacity(if self.max_one_row { 4 } else { 3 });
        vec.push(("type", Pretty::debug(&self.join_type)));

        let concat_schema = self.concat_schema();
        let cond = Pretty::debug(&ConditionDisplay {
            condition: &self.on,
            input_schema: &concat_schema,
        });
        vec.push(("on", cond));

        vec.push(("correlated_id", Pretty::debug(&self.correlated_id)));
        if self.max_one_row {
            vec.push(("max_one_row", Pretty::debug(&true)));
        }

        childless_record("LogicalApply", vec)
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
        translated: bool,
    ) -> Self {
        let ctx = left.ctx();
        let join_core = generic::Join::with_full_output(left, right, join_type, on);
        let schema = join_core.schema();
        let stream_key = join_core.stream_key();
        let functional_dependency = match &stream_key {
            Some(stream_key) => FunctionalDependencySet::with_key(schema.len(), stream_key),
            None => FunctionalDependencySet::new(schema.len()),
        };
        let (left, right, on, join_type, _output_indices) = join_core.decompose();
        let base = PlanBase::new_logical(ctx, schema, stream_key, functional_dependency);
        LogicalApply {
            base,
            left,
            right,
            on,
            join_type,
            correlated_id,
            correlated_indices,
            max_one_row,
            translated,
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
            false,
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

    pub fn translated(&self) -> bool {
        self.translated
    }

    pub fn max_one_row(&self) -> bool {
        self.max_one_row
    }

    /// Translate Apply.
    ///
    /// Used to convert other kinds of Apply to cross Apply.
    ///
    /// Before:
    ///
    /// ```text
    ///     LogicalApply
    ///    /            \
    ///  LHS           RHS
    /// ```
    ///
    /// After:
    ///
    /// ```text
    ///      LogicalJoin
    ///    /            \
    ///  LHS        LogicalApply
    ///             /           \
    ///          Domain         RHS
    /// ```
    pub fn translate_apply(self, domain: PlanRef, eq_predicates: Vec<ExprImpl>) -> PlanRef {
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

        let new_apply = LogicalApply::new(
            domain,
            apply_right,
            JoinType::Inner,
            Condition::true_cond(),
            correlated_id,
            correlated_indices,
            max_one_row,
            true,
        )
        .into();

        let on = Self::rewrite_on(on, correlated_indices_len, apply_left_len).and(Condition {
            conjunctions: eq_predicates,
        });
        let new_join = LogicalJoin::new(apply_left, new_apply, apply_type, on);

        if new_join.join_type() == JoinType::LeftSemi {
            // Schema doesn't change, still LHS.
            new_join.into()
        } else {
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

    fn concat_schema(&self) -> Schema {
        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        Schema::new(concat_schema)
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
            self.translated,
        )
    }
}

impl_plan_tree_node_for_binary! { LogicalApply }

impl ColPrunable for LogicalApply {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl ExprRewritable for LogicalApply {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new = self.clone();
        new.on = new.on.rewrite_expr(r);
        new.base = new.base.clone_with_new_plan_id();
        new.into()
    }
}

impl ExprVisitable for LogicalApply {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.on.visit_expr(v)
    }
}

impl PredicatePushdown for LogicalApply {
    fn predicate_pushdown(
        &self,
        mut predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let left_col_num = self.left().schema().len();
        let right_col_num = self.right().schema().len();
        let join_type = self.join_type();

        let (left_from_filter, right_from_filter, on) =
            push_down_into_join(&mut predicate, left_col_num, right_col_num, join_type, true);

        let mut new_on = self.on.clone().and(on);
        let (left_from_on, right_from_on) =
            push_down_join_condition(&mut new_on, left_col_num, right_col_num, join_type, true);

        let left_predicate = left_from_filter.and(left_from_on);
        let right_predicate = right_from_filter.and(right_from_on);

        let new_left = self.left().predicate_pushdown(left_predicate, ctx);
        let new_right = self.right().predicate_pushdown(right_predicate, ctx);

        let new_apply = LogicalApply::create(
            new_left,
            new_right,
            join_type,
            new_on,
            self.correlated_id,
            self.correlated_indices.clone(),
            self.max_one_row,
        );
        LogicalFilter::create(new_apply, predicate)
    }
}

impl ToBatch for LogicalApply {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_owned(),
        )))
    }
}

impl ToStream for LogicalApply {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_owned(),
        )))
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_owned(),
        )))
    }
}
