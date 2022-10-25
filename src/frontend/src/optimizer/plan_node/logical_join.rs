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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    generic, BatchProject, ColPrunable, CollectInputRef, LogicalProject, PlanBase, PlanNodeType,
    PlanRef, PlanTreeNodeBinary, PredicatePushdown, StreamHashJoin, StreamProject, ToBatch,
    ToStream,
};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, InputRef};
use crate::optimizer::max_one_row_visitor::MaxOneRowVisitor;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    BatchFilter, BatchHashJoin, BatchLookupJoin, BatchNestedLoopJoin, EqJoinPredicate,
    LogicalFilter, StreamDynamicFilter, StreamFilter,
};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalJoin` combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub base: PlanBase,
    core: generic::Join<PlanRef>,
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("LogicalJoin");
        builder.field("type", &format_args!("{:?}", self.join_type()));

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "on",
            &format_args!(
                "{}",
                ConditionDisplay {
                    condition: self.on(),
                    input_schema: &concat_schema
                }
            ),
        );

        if verbose {
            if self
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: self.output_indices(),
                        input_schema: &concat_schema,
                    },
                );
            }
        }

        builder.finish()
    }
}

impl LogicalJoin {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType, on: Condition) -> Self {
        let out_column_num =
            Self::out_column_num(left.schema().len(), right.schema().len(), join_type);
        Self::with_output_indices(left, right, join_type, on, (0..out_column_num).collect())
    }

    pub(crate) fn with_output_indices(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        output_indices: Vec<usize>,
    ) -> Self {
        let ctx = left.ctx();
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type, &output_indices);

        let pk_indices = Self::derive_pk(
            left.schema().len(),
            right.schema().len(),
            left.logical_pk(),
            right.logical_pk(),
            join_type,
            &output_indices,
        );

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        let pk_indices = pk_indices.and_then(|mut pk_indices| {
            let left_len = left.schema().len();
            let right_len = right.schema().len();
            let eq_predicate = EqJoinPredicate::create(left_len, right_len, on.clone());

            let l2i = Self::l2i_col_mapping_inner(left_len, right_len, join_type);
            let r2i = Self::r2i_col_mapping_inner(left_len, right_len, join_type);
            let out_col_num = Self::out_column_num(left_len, right_len, join_type);
            let i2o = ColIndexMapping::with_remaining_columns(&output_indices, out_col_num);

            for (lk, rk) in eq_predicate.eq_indexes() {
                if let Some(lk) = l2i.try_map(lk) {
                    let out_k = i2o.try_map(lk)?;
                    if !pk_indices.contains(&out_k) {
                        pk_indices.push(out_k);
                    }
                }
                if let Some(rk) = r2i.try_map(rk) {
                    let out_k = i2o.try_map(rk)?;
                    if !pk_indices.contains(&out_k) {
                        pk_indices.push(out_k);
                    }
                }
            }
            Some(pk_indices)
        });
        // NOTE(st1page) over
        let functional_dependency = Self::derive_fd(
            left.schema().len(),
            right.schema().len(),
            left.functional_dependency().clone(),
            right.functional_dependency().clone(),
            &on,
            join_type,
            &output_indices,
        );
        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        // let pk_indices = match pk_indices {
        //     Some(pk_indices) if functional_dependency.is_key(&pk_indices) => {
        //         functional_dependency.minimize_key(&pk_indices)
        //     }
        //     _ => pk_indices.unwrap_or_default(),
        // };
        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );
        let core = generic::Join::new(left, right, on, join_type, output_indices);
        LogicalJoin { base, core }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: ExprImpl,
    ) -> PlanRef {
        Self::new(left, right, join_type, Condition::with_expr(on_clause)).into()
    }

    pub fn out_column_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
            JoinType::Unspecified => unreachable!(),
        }
    }

    pub fn internal_column_num(&self) -> usize {
        Self::out_column_num(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    fn i2l_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::empty(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    fn i2r_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    fn l2i_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::i2l_col_mapping_inner(left_len, right_len, join_type).inverse()
    }

    fn r2i_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::i2r_col_mapping_inner(left_len, right_len, join_type).inverse()
    }

    /// get the Mapping of columnIndex from internal column index to left column index
    pub fn i2l_col_mapping(&self) -> ColIndexMapping {
        Self::i2l_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from internal column index to right column index
    pub fn i2r_col_mapping(&self) -> ColIndexMapping {
        Self::i2r_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from left column index to internal column index
    pub fn l2i_col_mapping(&self) -> ColIndexMapping {
        Self::l2i_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from right column index to internal column index
    pub fn r2i_col_mapping(&self) -> ColIndexMapping {
        Self::r2i_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(self.output_indices(), self.internal_column_num())
    }

    /// get the Mapping of columnIndex from output column index to internal column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.i2o_col_mapping().inverse()
    }

    pub(super) fn derive_schema(
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: JoinType,
        output_indices: &[usize],
    ) -> Schema {
        let left_len = left_schema.len();
        let right_len = right_schema.len();
        let i2l = Self::i2l_col_mapping_inner(left_len, right_len, join_type);
        let i2r = Self::i2r_col_mapping_inner(left_len, right_len, join_type);
        let fields = output_indices
            .iter()
            .map(|&i| match (i2l.try_map(i), i2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(
                    "left len {}, right len {}, i {}, lmap {:?}, rmap {:?}",
                    left_len, right_len, i, i2l, i2r
                ),
            })
            .collect();
        Schema { fields }
    }

    pub(super) fn derive_pk(
        left_len: usize,
        right_len: usize,
        left_pk: &[usize],
        right_pk: &[usize],
        join_type: JoinType,
        output_indices: &[usize],
    ) -> Option<Vec<usize>> {
        let l2i = Self::l2i_col_mapping_inner(left_len, right_len, join_type);
        let r2i = Self::r2i_col_mapping_inner(left_len, right_len, join_type);
        let out_col_num = Self::out_column_num(left_len, right_len, join_type);
        let i2o = ColIndexMapping::with_remaining_columns(output_indices, out_col_num);
        left_pk
            .iter()
            .map(|index| l2i.try_map(*index))
            .chain(right_pk.iter().map(|index| r2i.try_map(*index)))
            .flatten()
            .map(|index| i2o.try_map(index))
            .collect::<Option<Vec<_>>>()
    }

    pub(super) fn derive_fd(
        left_len: usize,
        right_len: usize,
        left_fd_set: FunctionalDependencySet,
        right_fd_set: FunctionalDependencySet,
        on: &Condition,
        join_type: JoinType,
        output_indices: &[usize],
    ) -> FunctionalDependencySet {
        let out_col_num = Self::out_column_num(left_len, right_len, join_type);

        let get_new_left_fd_set = |left_fd_set: FunctionalDependencySet| {
            ColIndexMapping::with_shift_offset(left_len, 0)
                .composite(&ColIndexMapping::identity(out_col_num))
                .rewrite_functional_dependency_set(left_fd_set)
        };
        let get_new_right_fd_set = |right_fd_set: FunctionalDependencySet| {
            ColIndexMapping::with_shift_offset(right_len, left_len.try_into().unwrap())
                .rewrite_functional_dependency_set(right_fd_set)
        };
        let fd_set: FunctionalDependencySet = match join_type {
            JoinType::Inner => {
                let mut fd_set = FunctionalDependencySet::new(out_col_num);
                for i in &on.conjunctions {
                    if let Some((col, _)) = i.as_eq_const() {
                        fd_set.add_constant_columns(&[col.index()])
                    } else if let Some((left, right)) = i.as_eq_cond() {
                        fd_set.add_functional_dependency_by_column_indices(
                            &[left.index()],
                            &[right.index()],
                        );
                        fd_set.add_functional_dependency_by_column_indices(
                            &[right.index()],
                            &[left.index()],
                        );
                    }
                }
                get_new_left_fd_set(left_fd_set)
                    .into_dependencies()
                    .into_iter()
                    .chain(
                        get_new_right_fd_set(right_fd_set)
                            .into_dependencies()
                            .into_iter(),
                    )
                    .for_each(|fd| fd_set.add_functional_dependency(fd));
                fd_set
            }
            JoinType::LeftOuter => get_new_left_fd_set(left_fd_set),
            JoinType::RightOuter => get_new_right_fd_set(right_fd_set),
            JoinType::FullOuter => FunctionalDependencySet::new(out_col_num),
            JoinType::LeftSemi | JoinType::LeftAnti => left_fd_set,
            JoinType::RightSemi | JoinType::RightAnti => right_fd_set,
            JoinType::Unspecified => unreachable!(),
        };
        ColIndexMapping::with_remaining_columns(output_indices, out_col_num)
            .rewrite_functional_dependency_set(fd_set)
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.core.on
    }

    /// Get the join type of the logical join.
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    /// Get the output indices of the logical join.
    pub fn output_indices(&self) -> &Vec<usize> {
        &self.core.output_indices
    }

    /// Clone with new output indices
    pub fn clone_with_output_indices(&self, output_indices: Vec<usize>) -> Self {
        Self::with_output_indices(
            self.left().clone(),
            self.right().clone(),
            self.join_type(),
            self.on().clone(),
            output_indices,
        )
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::with_output_indices(
            self.left().clone(),
            self.right().clone(),
            self.join_type(),
            cond,
            self.output_indices().clone(),
        )
    }

    pub fn is_left_join(&self) -> bool {
        matches!(self.join_type(), JoinType::LeftSemi | JoinType::LeftAnti)
    }

    pub fn is_right_join(&self) -> bool {
        matches!(self.join_type(), JoinType::RightSemi | JoinType::RightAnti)
    }

    /// Try to split and pushdown `predicate` into a join's left/right child or the on clause.
    /// Returns the pushed predicates. The pushed part will be removed from the original predicate.
    ///
    /// `InputRef`s in the right `Condition` are shifted by `-left_col_num`.
    fn push_down(
        predicate: &mut Condition,
        left_col_num: usize,
        right_col_num: usize,
        push_left: bool,
        push_right: bool,
        push_on: bool,
    ) -> (Condition, Condition, Condition) {
        let conjunctions = std::mem::take(&mut predicate.conjunctions);
        let (mut left, right, mut others) =
            Condition { conjunctions }.split(left_col_num, right_col_num);

        let mut cannot_push = vec![];

        if !push_left {
            cannot_push.extend(left);
            left = Condition::true_cond();
        };

        let right = if push_right {
            let mut mapping = ColIndexMapping::with_shift_offset(
                left_col_num + right_col_num,
                -(left_col_num as isize),
            );
            right.rewrite_expr(&mut mapping)
        } else {
            cannot_push.extend(right);
            Condition::true_cond()
        };

        let on = if push_on {
            others.conjunctions.extend(std::mem::take(&mut cannot_push));
            others
        } else {
            cannot_push.extend(others);
            Condition::true_cond()
        };

        predicate.conjunctions = cannot_push;

        (left, right, on)
    }

    fn can_push_left_from_filter(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
        )
    }

    fn can_push_right_from_filter(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn can_push_on_from_filter(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    }

    fn can_push_left_from_on(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi
        )
    }

    fn can_push_right_from_on(ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightSemi
        )
    }

    /// Try to simplify the outer join with the predicate on the top of the join
    ///
    /// now it is just a naive implementation for comparison expression, we can give a more general
    /// implementation with constant folding in future
    fn simplify_outer(predicate: &Condition, left_col_num: usize, join_type: JoinType) -> JoinType {
        let (mut gen_null_in_left, mut gen_null_in_right) = match join_type {
            JoinType::LeftOuter => (false, true),
            JoinType::RightOuter => (true, false),
            JoinType::FullOuter => (true, true),
            _ => return join_type,
        };

        for expr in &predicate.conjunctions {
            if let ExprImpl::FunctionCall(func) = expr {
                match func.get_expr_type() {
                    ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual => {
                        for input in func.inputs() {
                            if let ExprImpl::InputRef(input) = input {
                                let idx = input.index;
                                if idx < left_col_num {
                                    gen_null_in_left = false;
                                } else {
                                    gen_null_in_right = false;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
        }

        match (gen_null_in_left, gen_null_in_right) {
            (true, true) => JoinType::FullOuter,
            (true, false) => JoinType::RightOuter,
            (false, true) => JoinType::LeftOuter,
            (false, false) => JoinType::Inner,
        }
    }

    fn convert_to_lookup_join(
        &self,
        logical_join: LogicalJoin,
        mut predicate: EqJoinPredicate,
    ) -> Option<PlanRef> {
        let right = self.right();
        if right.as_ref().node_type() != PlanNodeType::LogicalScan {
            tracing::warn!(
                "Lookup Join only supports basic tables on the join's right side. A \
            different join will be used instead."
            );
            return None;
        }

        match logical_join.join_type() {
            JoinType::RightOuter
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::FullOuter => return None,
            _ => {}
        };

        let logical_scan = right.as_logical_scan().unwrap();
        let table_desc = logical_scan.table_desc().clone();
        let output_column_ids = logical_scan.output_column_ids();

        // Verify that the right join key columns are the same as the primary key
        // TODO: Refactor Lookup Join so that prefixes of the primary key are allowed
        let eq_col_warn_message = "In Lookup Join, the right columns of the equality join \
        predicates must be the same as the primary key. A different join will be used instead.";

        let order_col_ids = table_desc.order_column_ids();
        if order_col_ids.len() != predicate.right_eq_indexes().len() {
            tracing::warn!("{}", eq_col_warn_message);
            return None;
        }

        for (order_col_id, eq_idx) in order_col_ids
            .into_iter()
            .zip_eq(predicate.right_eq_indexes())
        {
            if order_col_id != output_column_ids[eq_idx] {
                tracing::warn!("{}", eq_col_warn_message);
                return None;
            }
        }

        let left_schema_len = logical_join.left().schema().len();
        struct Rewriter {
            offset: usize,
        }
        impl ExprRewriter for Rewriter {
            fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
                InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
            }
        }
        let mut rewriter = Rewriter {
            offset: left_schema_len,
        };

        let new_other = predicate
            .other_cond()
            .clone()
            .and(logical_scan.predicate().clone().rewrite_expr(&mut rewriter));
        *predicate.other_cond_mut() = new_other;

        Some(BatchLookupJoin::new(logical_join, predicate, table_desc, output_column_ids).into())
    }

    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        self.core.decompose()
    }
}

impl PlanTreeNodeBinary for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::with_output_indices(
            left,
            right,
            self.join_type(),
            self.on().clone(),
            self.output_indices().clone(),
        )
    }

    #[must_use]
    fn rewrite_with_left_right(
        &self,
        left: PlanRef,
        left_col_change: ColIndexMapping,
        right: PlanRef,
        right_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let (new_on, new_output_indices) = {
            let (mut map, _) = left_col_change.clone().into_parts();
            let (mut right_map, _) = right_col_change.clone().into_parts();
            for i in right_map.iter_mut().flatten() {
                *i += left.schema().len();
            }
            map.append(&mut right_map);
            let mut mapping = ColIndexMapping::new(map);

            let new_output_indices = self
                .output_indices()
                .iter()
                .map(|&i| mapping.map(i))
                .collect::<Vec<_>>();
            let new_on = self.on().clone().rewrite_expr(&mut mapping);
            (new_on, new_output_indices)
        };

        let join = Self::with_output_indices(
            left,
            right,
            self.join_type(),
            new_on,
            new_output_indices.clone(),
        );

        let new_i2o = ColIndexMapping::with_remaining_columns(
            &new_output_indices,
            join.internal_column_num(),
        );

        let old_o2i = self.o2i_col_mapping();

        let old_i2l = old_o2i
            .composite(&self.i2l_col_mapping())
            .composite(&left_col_change);
        let old_i2r = old_o2i
            .composite(&self.i2r_col_mapping())
            .composite(&right_col_change);
        let new_l2i = join.l2i_col_mapping().composite(&new_i2o);
        let new_r2i = join.r2i_col_mapping().composite(&new_i2o);

        let out_col_change = old_i2l
            .composite(&new_l2i)
            .union(&old_i2r.composite(&new_r2i));
        (join, out_col_change)
    }
}

impl_plan_tree_node_for_binary! { LogicalJoin }

impl ColPrunable for LogicalJoin {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        // make `required_cols` point to internal table instead of output schema.
        let required_cols = required_cols
            .iter()
            .map(|i| self.output_indices()[*i])
            .collect_vec();
        let left_len = self.left().schema().fields.len();

        let total_len = self.left().schema().len() + self.right().schema().len();
        let mut resized_required_cols = FixedBitSet::with_capacity(total_len);

        required_cols.iter().for_each(|&i| {
            if self.is_right_join() {
                resized_required_cols.insert(left_len + i);
            } else {
                resized_required_cols.insert(i);
            }
        });

        // add those columns which are required in the join condition to
        // to those that are required in the output
        let mut visitor = CollectInputRef::new(resized_required_cols);
        self.on().visit_expr(&mut visitor);
        let left_right_required_cols = FixedBitSet::from(visitor).ones().collect_vec();

        let mut left_required_cols = Vec::new();
        let mut right_required_cols = Vec::new();
        left_right_required_cols.iter().for_each(|&i| {
            if i < left_len {
                left_required_cols.push(i);
            } else {
                right_required_cols.push(i - left_len);
            }
        });

        let mut on = self.on().clone();
        let mut mapping =
            ColIndexMapping::with_remaining_columns(&left_right_required_cols, total_len);
        on = on.rewrite_expr(&mut mapping);

        let new_output_indices = {
            let required_inputs_in_output = if self.is_left_join() {
                &left_required_cols
            } else if self.is_right_join() {
                &right_required_cols
            } else {
                &left_right_required_cols
            };

            let mapping =
                ColIndexMapping::with_remaining_columns(required_inputs_in_output, total_len);
            required_cols.iter().map(|&i| mapping.map(i)).collect_vec()
        };

        LogicalJoin::with_output_indices(
            self.left().prune_col(&left_required_cols),
            self.right().prune_col(&right_required_cols),
            self.join_type(),
            on,
            new_output_indices,
        )
        .into()
    }
}

impl PredicatePushdown for LogicalJoin {
    /// Pushes predicates above and within a join node into the join node and/or its children nodes.
    ///
    /// # Which predicates can be pushed
    ///
    /// For inner join, we can do all kinds of pushdown.
    ///
    /// For left/right semi join, we can push filter to left/right and on-clause,
    /// and push on-clause to left/right.
    ///
    /// For left/right anti join, we can push filter to left/right, but on-clause can not be pushed
    ///
    /// ## Outer Join
    ///
    /// Preserved Row table
    /// : The table in an Outer Join that must return all rows.
    ///
    /// Null Supplying table
    /// : This is the table that has nulls filled in for its columns in unmatched rows.
    ///
    /// |                          | Preserved Row table | Null Supplying table |
    /// |--------------------------|---------------------|----------------------|
    /// | Join predicate (on)      | Not Pushed          | Pushed               |
    /// | Where predicate (filter) | Pushed              | Not Pushed           |
    fn predicate_pushdown(&self, mut predicate: Condition) -> PlanRef {
        let left_col_num = self.left().schema().len();
        let right_col_num = self.right().schema().len();
        let join_type = LogicalJoin::simplify_outer(&predicate, left_col_num, self.join_type());

        // rewrite output col referencing indices as internal cols
        let mut mapping = self.o2i_col_mapping();

        predicate = predicate.rewrite_expr(&mut mapping);

        let (left_from_filter, right_from_filter, on) = LogicalJoin::push_down(
            &mut predicate,
            left_col_num,
            right_col_num,
            LogicalJoin::can_push_left_from_filter(join_type),
            LogicalJoin::can_push_right_from_filter(join_type),
            LogicalJoin::can_push_on_from_filter(join_type),
        );

        let mut new_on = self.on().clone().and(on);
        let (left_from_on, right_from_on, on) = LogicalJoin::push_down(
            &mut new_on,
            left_col_num,
            right_col_num,
            LogicalJoin::can_push_left_from_on(join_type),
            LogicalJoin::can_push_right_from_on(join_type),
            false,
        );
        assert!(
            on.always_true(),
            "On-clause should not be pushed to on-clause."
        );

        let left_predicate = left_from_filter.and(left_from_on);
        let right_predicate = right_from_filter.and(right_from_on);

        let new_left = self.left().predicate_pushdown(left_predicate);
        let new_right = self.right().predicate_pushdown(right_predicate);
        let new_join = LogicalJoin::with_output_indices(
            new_left,
            new_right,
            join_type,
            new_on,
            self.output_indices().clone(),
        );

        let mut mapping = self.i2o_col_mapping();
        predicate = predicate.rewrite_expr(&mut mapping);
        LogicalFilter::create(new_join.into(), predicate)
    }
}

impl LogicalJoin {
    pub fn to_batch_lookup_join(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left().schema().len(),
            self.right().schema().len(),
            self.on().clone(),
        );

        let left = self.left().to_batch()?;
        let right = self.right().to_batch()?;
        let logical_join = self.clone_with_left_right(left, right);

        Ok(self
            .convert_to_lookup_join(logical_join, predicate)
            .expect("Fail to convert to lookup join"))
    }
}

impl ToBatch for LogicalJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left().schema().len(),
            self.right().schema().len(),
            self.on().clone(),
        );

        let left = self.left().to_batch()?;
        let right = self.right().to_batch()?;
        let logical_join = self.clone_with_left_right(left, right);

        let config = self.base.ctx.inner().session_ctx.config();

        if predicate.has_eq() {
            if config.get_batch_enable_lookup_join() {
                if let Some(lookup_join) =
                    self.convert_to_lookup_join(logical_join.clone(), predicate.clone())
                {
                    return Ok(lookup_join);
                }
            }

            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type() == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let new_output_indices = logical_join.output_indices().clone();
                let new_internal_column_num = logical_join.internal_column_num();
                let default_indices = (0..new_internal_column_num).collect::<Vec<_>>();
                let logical_join = logical_join.clone_with_output_indices(default_indices.clone());
                let eq_cond = EqJoinPredicate::new(
                    Condition::true_cond(),
                    predicate.eq_keys().to_vec(),
                    self.left().schema().len(),
                );
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = BatchHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                let plan = BatchFilter::new(logical_filter).into();
                if self.output_indices() != &default_indices {
                    let logical_project =
                        LogicalProject::with_out_col_idx(plan, new_output_indices.into_iter());
                    Ok(BatchProject::new(logical_project).into())
                } else {
                    Ok(plan)
                }
            } else {
                Ok(BatchHashJoin::new(logical_join, predicate).into())
            }
        } else {
            // Convert to Nested-loop Join for non-equal joins
            Ok(BatchNestedLoopJoin::new(logical_join).into())
        }
    }
}

impl ToStream for LogicalJoin {
    fn to_stream(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left().schema().len(),
            self.right().schema().len(),
            self.on().clone(),
        );

        if predicate.has_eq() {
            let mut right =
                self.right()
                    .to_stream_with_dist_required(&RequiredDist::shard_by_key(
                        self.right().schema().len(),
                        &predicate.right_eq_indexes(),
                    ))?;
            let mut left = self.left();

            let r2l = predicate.r2l_eq_columns_mapping(left.schema().len(), right.schema().len());
            let l2r = r2l.inverse();

            let right_dist = right.distribution();
            match right_dist {
                Distribution::HashShard(_) => {
                    let left_dist = r2l.rewrite_required_distribution(&RequiredDist::PhysicalDist(
                        right_dist.clone(),
                    ));
                    left = left.to_stream_with_dist_required(&left_dist)?;
                }
                Distribution::UpstreamHashShard(_) => {
                    left = left.to_stream_with_dist_required(&RequiredDist::shard_by_key(
                        self.left().schema().len(),
                        &predicate.left_eq_indexes(),
                    ))?;
                    let left_dist = left.distribution();
                    match left_dist {
                        Distribution::HashShard(_) => {
                            let right_dist = l2r.rewrite_required_distribution(
                                &RequiredDist::PhysicalDist(left_dist.clone()),
                            );
                            right = right_dist.enforce_if_not_satisfies(right, &Order::any())?
                        }
                        Distribution::UpstreamHashShard(_) => {
                            left = RequiredDist::hash_shard(&predicate.left_eq_indexes())
                                .enforce_if_not_satisfies(left, &Order::any())?;
                            right = RequiredDist::hash_shard(&predicate.right_eq_indexes())
                                .enforce_if_not_satisfies(right, &Order::any())?;
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }

            let logical_join = self.clone_with_left_right(left, right);

            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type() == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let default_indices = (0..self.internal_column_num()).collect::<Vec<_>>();

                // Temporarily remove output indices.
                let logical_join = logical_join.clone_with_output_indices(default_indices.clone());
                let eq_cond = EqJoinPredicate::new(
                    Condition::true_cond(),
                    predicate.eq_keys().to_vec(),
                    self.left().schema().len(),
                );
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = StreamHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                let plan = StreamFilter::new(logical_filter).into();
                if self.output_indices() != &default_indices {
                    let logical_project = LogicalProject::with_mapping(
                        plan,
                        ColIndexMapping::with_remaining_columns(
                            self.output_indices(),
                            self.internal_column_num(),
                        ),
                    );
                    Ok(StreamProject::new(logical_project).into())
                } else {
                    Ok(plan)
                }
            } else {
                Ok(StreamHashJoin::new(logical_join, predicate).into())
            }
        } else {
            let nested_loop_join_error = || {
                RwError::from(ErrorCode::NotImplemented(
                    "stream nested-loop join".to_string(),
                    None.into(),
                ))
            };
            // If there is exactly one predicate, it is a comparison (<, <=, >, >=), and the
            // join is a `Inner` join, we can convert the scalar subquery into a
            // `StreamDynamicFilter`

            // Check if `Inner` subquery (no `IN` or `EXISTS` keywords)
            if self.join_type() != JoinType::Inner {
                return Err(nested_loop_join_error());
            }

            // Check if right side is a scalar (for now, check if it is a simple agg)
            let maybe_simple_agg = if let Some(proj) = self.right().as_logical_project() {
                proj.input()
            } else {
                self.right()
            };

            // Check if right side is a scalar
            if !MaxOneRowVisitor.visit(self.right()) {
                return Err(nested_loop_join_error());
            }

            // Check if the join condition is a correlated comparison
            let conj = &predicate.other_cond().conjunctions;
            let left_ref_index = if let [expr] = conj.as_slice() {
                if let Some((left_ref, _, right_ref)) = expr.as_comparison_cond()
                    && left_ref.index < self.left().schema().len()
                    && right_ref.index >= self.left().schema().len()
                {
                    let left_datatype = &self.left().schema().data_types()[left_ref.index];
                    let right_index = right_ref.index - self.left().schema().len();
                    let right_datatype = &self.right().schema().data_types()[right_index];
                    // We align input types on all join predicates with cmp operator
                    assert_eq!(left_datatype, right_datatype);
                    left_ref.index
                } else {
                    return Err(nested_loop_join_error());
                }
            } else {
                return Err(nested_loop_join_error());
            };

            // Check if non of the columns from the inner side is required to output
            let all_output_from_left = self
                .output_indices()
                .iter()
                .all(|i| *i < self.left().schema().len());
            if !all_output_from_left {
                return Err(nested_loop_join_error());
            }

            let left = self.left().to_stream()?;

            let right = self
                .right()
                .to_stream_with_dist_required(&RequiredDist::PhysicalDist(
                    Distribution::Broadcast,
                ))?;

            assert!(right.as_stream_exchange().is_some());
            assert_eq!(
                *right.inputs().iter().exactly_one().unwrap().distribution(),
                Distribution::Single
            );

            let plan = StreamDynamicFilter::new(
                left_ref_index,
                predicate.other_cond().clone(),
                left,
                right,
            )
            .into();

            // TODO: `DynamicFilterExecutor` should support `output_indices` in `ChunkBuilder`
            if self
                .output_indices()
                .iter()
                .copied()
                .ne(0..self.left().schema().len())
            {
                // The schema of dynamic filter is always the same as the left side now, and we have
                // checked that all output columns are from the left side before.
                let logical_project = LogicalProject::with_mapping(
                    plan,
                    ColIndexMapping::with_remaining_columns(
                        self.output_indices(),
                        self.left().schema().len(),
                    ),
                );
                Ok(StreamProject::new(logical_project).into())
            } else {
                Ok(plan)
            }
        }
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (left, left_col_change) = self.left().logical_rewrite_for_stream()?;
        let left_len = left.schema().len();
        let (right, right_col_change) = self.right().logical_rewrite_for_stream()?;
        let (join, out_col_change) = self.rewrite_with_left_right(
            left.clone(),
            left_col_change,
            right.clone(),
            right_col_change,
        );

        let mapping = ColIndexMapping::with_remaining_columns(
            join.output_indices(),
            join.internal_column_num(),
        );

        let l2i = join.l2i_col_mapping().composite(&mapping);
        let r2i = join.r2i_col_mapping().composite(&mapping);

        // Add missing pk indices to the logical join
        let left_to_add = left
            .logical_pk()
            .iter()
            .cloned()
            .filter(|i| l2i.try_map(*i).is_none());

        let right_to_add = right
            .logical_pk()
            .iter()
            .cloned()
            .filter(|i| r2i.try_map(*i).is_none())
            .map(|i| i + left_len);

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        let right_len = right.schema().len();
        let eq_predicate = EqJoinPredicate::create(left_len, right_len, join.on().clone());

        let left_to_add = left_to_add
            .chain(
                eq_predicate
                    .left_eq_indexes()
                    .into_iter()
                    .filter(|i| l2i.try_map(*i).is_none()),
            )
            .unique();
        let right_to_add = right_to_add
            .chain(
                eq_predicate
                    .right_eq_indexes()
                    .into_iter()
                    .filter(|i| r2i.try_map(*i).is_none())
                    .map(|i| i + left_len),
            )
            .unique();
        // NOTE(st1page) over

        let mut new_output_indices = join.output_indices().clone();
        if !join.is_right_join() {
            new_output_indices.extend(left_to_add);
        }
        if !join.is_left_join() {
            new_output_indices.extend(right_to_add);
        }

        let join_with_pk = join.clone_with_output_indices(new_output_indices);
        // the added columns is at the end, so it will not change the exists column index
        Ok((join_with_pk.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, Datum};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::{LogicalValues, PlanTreeNodeUnary};
    use crate::optimizer::property::FunctionalDependency;
    use crate::session::OptimizerContext;

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [2,3] will result in
    /// ```text
    /// Project(input_ref(1), input_ref(2))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_prune_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let required_cols = vec![2, 3];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[2]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Semi join panicked previously at `prune_col`. Add test to prevent regression.
    #[tokio::test]
    async fn test_prune_semi_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(4, ty))),
                ],
            )
            .unwrap(),
        ));
        for join_type in [
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ] {
            let join = LogicalJoin::new(
                left.clone().into(),
                right.clone().into(),
                join_type,
                Condition::with_expr(on.clone()),
            );

            let offset = if join.is_right_join() { 3 } else { 0 };

            // Perform the prune
            let required_cols = vec![0];
            // key 0 is never used in the join (always key 1)
            let plan = join.prune_col(&required_cols);
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 1);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);

            // Perform the prune
            let required_cols = vec![0, 1, 2];
            // should not panic here
            let plan = join.prune_col(&required_cols);
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 3);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);
            assert_eq!(as_plan.schema().fields()[1], fields[offset + 1]);
            assert_eq!(as_plan.schema().fields()[2], fields[offset + 2]);
        }
    }

    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [1,3] will result in
    /// ```text
    /// Join(on: input_ref(0)=input_ref(1))
    ///   TableScan(v2)
    ///   TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_prune_join_no_project() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let required_cols = vec![1, 3];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[1]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 1);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..2]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Convert
    /// ```text
    /// Join(on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// Filter($2 == 42)
    ///   HashJoin(on: $1 = $3)
    ///     TableScan(v1, v2, v3)
    ///     TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    async fn test_join_to_batch() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(DataType::Int32, format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );

        fn input_ref(i: usize) -> ExprImpl {
            ExprImpl::InputRef(Box::new(InputRef::new(i, DataType::Int32)))
        }
        let eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::Equal, vec![input_ref(1), input_ref(3)]).unwrap(),
        ));
        let non_eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    input_ref(2),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Datum::Some(42_i32.into()),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));
        // Condition: ($1 = $3) AND ($2 == 42)
        let on_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::And, vec![eq_cond.clone(), non_eq_cond.clone()]).unwrap(),
        ));

        let join_type = JoinType::Inner;
        let logical_join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on_cond),
        );

        // Perform `to_batch`
        let result = logical_join.to_batch().unwrap();

        // Expected plan: Filter($2 == 42) --> HashJoin($1 = $3)
        let batch_filter = result.as_batch_filter().unwrap();
        assert_eq!(
            ExprImpl::from(batch_filter.predicate().clone()),
            non_eq_cond
        );

        let input = batch_filter.input();
        let hash_join = input.as_batch_hash_join().unwrap();
        assert_eq!(
            ExprImpl::from(hash_join.eq_join_predicate().eq_cond()),
            eq_cond
        );
    }

    /// Convert
    /// ```text
    /// Join(join_type: left outer, on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// HashJoin(join_type: left outer, on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    #[ignore] // ignore due to refactor logical scan, but the test seem to duplicate with the explain test
              // framework, maybe we will remove it?
    async fn test_join_to_stream() {
        // let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        // let fields: Vec<Field> = (1..7)
        //     .map(|i| Field {
        //         data_type: DataType::Int32,
        //         name: format!("v{}", i),
        //     })
        //     .collect();
        // let left = LogicalScan::new(
        //     "left".to_string(),
        //     TableId::new(0),
        //     vec![1.into(), 2.into(), 3.into()],
        //     Schema {
        //         fields: fields[0..3].to_vec(),
        //     },
        //     ctx.clone(),
        // );
        // let right = LogicalScan::new(
        //     "right".to_string(),
        //     TableId::new(0),
        //     vec![4.into(), 5.into(), 6.into()],
        //     Schema {
        //         fields: fields[3..6].to_vec(),
        //     },
        //     ctx,
        // );
        // let eq_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(
        //         Type::Equal,
        //         vec![
        //             ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
        //             ExprImpl::InputRef(Box::new(InputRef::new(3, DataType::Int32))),
        //         ],
        //     )
        //     .unwrap(),
        // ));
        // let non_eq_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(
        //         Type::Equal,
        //         vec![
        //             ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
        //             ExprImpl::Literal(Box::new(Literal::new(
        //                 Datum::Some(42_i32.into()),
        //                 DataType::Int32,
        //             ))),
        //         ],
        //     )
        //     .unwrap(),
        // ));
        // // Condition: ($1 = $3) AND ($2 == 42)
        // let on_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(Type::And, vec![eq_cond, non_eq_cond]).unwrap(),
        // ));

        // let join_type = JoinType::LeftOuter;
        // let logical_join = LogicalJoin::new(
        //     left.into(),
        //     right.into(),
        //     join_type,
        //     Condition::with_expr(on_cond.clone()),
        // );

        // // Perform `to_stream`
        // let result = logical_join.to_stream();

        // // Expected plan: HashJoin(($1 = $3) AND ($2 == 42))
        // let hash_join = result.as_stream_hash_join().unwrap();
        // assert_eq!(hash_join.eq_join_predicate().all_cond().as_expr(), on_cond);
    }
    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [3, 2] will result in
    /// ```text
    /// Project(input_ref(2), input_ref(1))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_join_column_prune_with_order_required() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let required_cols = vec![3, 2];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[3]);
        assert_eq!(join.schema().fields()[1], fields[2]);

        let expr: ExprImpl = join.on().clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    #[tokio::test]
    async fn fd_derivation_inner_outer_join() {
        // left: [l0, l1], right: [r0, r1, r2]
        // FD: l0 --> l1, r0 --> { r1, r2 }
        // On: l0 = 0 AND l1 = r1
        //
        // Inner Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: l0 --> l1, r0 --> { r1, r2 }, {} --> l0, l1 --> r1, r1 --> l1
        // Left Outer Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: l0 --> l1
        // Right Outer Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: r0 --> { r1, r2 }
        // Full Outer Join:
        //  Schema: [l0, l1, r0, r1, r2]
        //  FD: empty
        // Left Semi/Anti Join:
        //  Schema: [l0, l1]
        //  FD: l0 --> l1
        // Right Semi/Anti Join:
        //  Schema: [r0, r1, r2]
        //  FD: r0 --> {r1, r2}
        let ctx = OptimizerContext::mock().await;
        let left = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "l0"),
                Field::with_name(DataType::Int32, "l1"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx.clone());
            // 0 --> 1
            values
                .base
                .functional_dependency
                .add_functional_dependency_by_column_indices(&[0], &[1]);
            values
        };
        let right = {
            let fields: Vec<Field> = vec![
                Field::with_name(DataType::Int32, "r0"),
                Field::with_name(DataType::Int32, "r1"),
                Field::with_name(DataType::Int32, "r2"),
            ];
            let mut values = LogicalValues::new(vec![], Schema { fields }, ctx);
            // 0 --> 1, 2
            values
                .base
                .functional_dependency
                .add_functional_dependency_by_column_indices(&[0], &[1, 2]);
            values
        };
        // l0 = 0 AND l1 = r1
        let on: ExprImpl = FunctionCall::new(
            Type::And,
            vec![
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        InputRef::new(0, DataType::Int32).into(),
                        ExprImpl::literal_int(0),
                    ],
                )
                .unwrap()
                .into(),
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        InputRef::new(1, DataType::Int32).into(),
                        InputRef::new(3, DataType::Int32).into(),
                    ],
                )
                .unwrap()
                .into(),
            ],
        )
        .unwrap()
        .into();
        let expected_fd_set = [
            (
                JoinType::Inner,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(5, &[0], &[1]),
                    // inherit from right
                    FunctionalDependency::with_indices(5, &[2], &[3, 4]),
                    // constant column in join condition
                    FunctionalDependency::with_indices(5, &[], &[0]),
                    // eq column in join condition
                    FunctionalDependency::with_indices(5, &[1], &[3]),
                    FunctionalDependency::with_indices(5, &[3], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (JoinType::FullOuter, HashSet::new()),
            (
                JoinType::RightOuter,
                [
                    // inherit from right
                    FunctionalDependency::with_indices(5, &[2], &[3, 4]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::LeftOuter,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(5, &[0], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::LeftSemi,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(2, &[0], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::LeftAnti,
                [
                    // inherit from left
                    FunctionalDependency::with_indices(2, &[0], &[1]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::RightSemi,
                [
                    // inherit from right
                    FunctionalDependency::with_indices(3, &[0], &[1, 2]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
            (
                JoinType::RightAnti,
                [
                    // inherit from right
                    FunctionalDependency::with_indices(3, &[0], &[1, 2]),
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
            ),
        ];

        for (join_type, expected_res) in expected_fd_set {
            let join = LogicalJoin::new(
                left.clone().into(),
                right.clone().into(),
                join_type,
                Condition::with_expr(on.clone()),
            );
            let fd_set = join
                .functional_dependency()
                .as_dependencies()
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            assert_eq!(fd_set, expected_res);
        }
    }
}
