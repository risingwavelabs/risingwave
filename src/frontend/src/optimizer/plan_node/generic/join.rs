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

use itertools::EitherOrBoth;
use risingwave_common::catalog::Schema;
use risingwave_pb::plan_common::JoinType;

use super::{EqJoinPredicate, GenericPlanNode, GenericPlanRef};
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition};

/// [`Join`] combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join<PlanRef> {
    pub left: PlanRef,
    pub right: PlanRef,
    pub on: Condition,
    pub join_type: JoinType,
    pub output_indices: Vec<usize>,
}

pub(crate) fn has_repeated_element(slice: &[usize]) -> bool {
    (1..slice.len()).any(|i| slice[i..].contains(&slice[i - 1]))
}

impl<PlanRef> Join<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.on = self.on.clone().rewrite_expr(r);
    }

    pub fn new(
        left: PlanRef,
        right: PlanRef,
        on: Condition,
        join_type: JoinType,
        output_indices: Vec<usize>,
    ) -> Self {
        // We cannot deal with repeated output indices in join
        debug_assert!(!has_repeated_element(&output_indices));
        Self {
            left,
            right,
            on,
            join_type,
            output_indices,
        }
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Join<PlanRef> {
    fn schema(&self) -> Schema {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let i2l = self.i2l_col_mapping();
        let i2r = self.i2r_col_mapping();
        let fields = self
            .output_indices
            .iter()
            .map(|&i| match (i2l.try_map(i), i2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(
                    "left len {}, right len {}, i {}, lmap {:?}, rmap {:?}",
                    left_schema.len(),
                    right_schema.len(),
                    i,
                    i2l,
                    i2r
                ),
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let _left_len = self.left.schema().len();
        let _right_len = self.right.schema().len();
        let left_pk = self.left.logical_pk();
        let right_pk = self.right.logical_pk();
        let l2i = self.l2i_col_mapping();
        let r2i = self.r2i_col_mapping();
        let full_out_col_num = self.internal_column_num();
        let i2o = ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

        let pk_indices = left_pk
            .iter()
            .map(|index| l2i.try_map(*index))
            .chain(right_pk.iter().map(|index| r2i.try_map(*index)))
            .flatten()
            .map(|index| i2o.try_map(index))
            .collect::<Option<Vec<_>>>();

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        pk_indices.and_then(|mut pk_indices| {
            let left_len = self.left.schema().len();
            let right_len = self.right.schema().len();
            let eq_predicate = EqJoinPredicate::create(left_len, right_len, self.on.clone());

            let l2i = self.l2i_col_mapping();
            let r2i = self.r2i_col_mapping();
            let full_out_col_num = self.internal_column_num();
            let i2o =
                ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

            let either_or_both = self.add_which_join_key_to_pk();

            for (lk, rk) in eq_predicate.eq_indexes() {
                match either_or_both {
                    EitherOrBoth::Left(_) => {
                        if let Some(lk) = l2i.try_map(lk) {
                            let out_k = i2o.try_map(lk)?;
                            if !pk_indices.contains(&out_k) {
                                pk_indices.push(out_k);
                            }
                        }
                    }
                    EitherOrBoth::Right(_) => {
                        if let Some(rk) = r2i.try_map(rk) {
                            let out_k = i2o.try_map(rk)?;
                            if !pk_indices.contains(&out_k) {
                                pk_indices.push(out_k);
                            }
                        }
                    }
                    EitherOrBoth::Both(_, _) => {
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
                };
            }
            Some(pk_indices)
        })
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.left.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();
        let left_fd_set = self.left.functional_dependency().clone();
        let right_fd_set = self.right.functional_dependency().clone();

        let full_out_col_num = self.internal_column_num();

        let get_new_left_fd_set = |left_fd_set: FunctionalDependencySet| {
            ColIndexMapping::with_shift_offset(left_len, 0)
                .composite(&ColIndexMapping::identity(full_out_col_num))
                .rewrite_functional_dependency_set(left_fd_set)
        };
        let get_new_right_fd_set = |right_fd_set: FunctionalDependencySet| {
            ColIndexMapping::with_shift_offset(right_len, left_len.try_into().unwrap())
                .rewrite_functional_dependency_set(right_fd_set)
        };
        let fd_set: FunctionalDependencySet = match self.join_type {
            JoinType::Inner => {
                let mut fd_set = FunctionalDependencySet::new(full_out_col_num);
                for i in &self.on.conjunctions {
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
            JoinType::FullOuter => FunctionalDependencySet::new(full_out_col_num),
            JoinType::LeftSemi | JoinType::LeftAnti => left_fd_set,
            JoinType::RightSemi | JoinType::RightAnti => right_fd_set,
            JoinType::Unspecified => unreachable!(),
        };
        ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num)
            .rewrite_functional_dependency_set(fd_set)
    }
}

impl<PlanRef> Join<PlanRef> {
    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        (
            self.left,
            self.right,
            self.on,
            self.join_type,
            self.output_indices,
        )
    }

    pub fn full_out_col_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
            JoinType::Unspecified => unreachable!(),
        }
    }
}

impl<PlanRef: GenericPlanRef> Join<PlanRef> {
    pub fn with_full_output(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
    ) -> Self {
        let out_column_num =
            Self::full_out_col_num(left.schema().len(), right.schema().len(), join_type);
        Self {
            left,
            right,
            join_type,
            on,
            output_indices: (0..out_column_num).collect(),
        }
    }

    pub fn internal_column_num(&self) -> usize {
        Self::full_out_col_num(
            self.left.schema().len(),
            self.right.schema().len(),
            self.join_type,
        )
    }

    pub fn is_full_out(&self) -> bool {
        self.output_indices.len() == self.internal_column_num()
    }

    /// Get the Mapping of columnIndex from internal column index to left column index.
    pub fn i2l_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => {
                ColIndexMapping::empty(right_len, left_len)
            }
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// Get the Mapping of columnIndex from internal column index to right column index.
    pub fn i2r_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len, right_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// TODO: This function may can be merged with `i2l_col_mapping` in future.
    pub fn i2l_col_mapping_ignore_join_type(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        ColIndexMapping::identity_or_none(left_len + right_len, left_len)
    }

    /// TODO: This function may can be merged with `i2r_col_mapping` in future.
    pub fn i2r_col_mapping_ignore_join_type(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
    }

    /// Get the Mapping of columnIndex from left column index to internal column index.
    pub fn l2i_col_mapping(&self) -> ColIndexMapping {
        self.i2l_col_mapping().inverse()
    }

    /// Get the Mapping of columnIndex from right column index to internal column index.
    pub fn r2i_col_mapping(&self) -> ColIndexMapping {
        self.i2r_col_mapping().inverse()
    }

    /// Get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(&self.output_indices, self.internal_column_num())
    }

    /// Get the Mapping of columnIndex from output column index to internal column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        // If output_indices = [0, 0, 1], we should use it as `o2i_col_mapping` directly.
        // If we use `self.i2o_col_mapping().inverse()`, we will lose the first 0.
        ColIndexMapping::new(self.output_indices.iter().map(|x| Some(*x)).collect())
    }

    pub fn add_which_join_key_to_pk(&self) -> EitherOrBoth<(), ()> {
        match self.join_type {
            JoinType::Inner => {
                // Theoretically adding either side is ok, but the distribution key of the inner
                // join derived based on the left side by default, so we choose the left side here
                // to ensure the pk comprises the distribution key.
                EitherOrBoth::Left(())
            }
            JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => EitherOrBoth::Left(()),
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightOuter => {
                EitherOrBoth::Right(())
            }
            JoinType::FullOuter => EitherOrBoth::Both((), ()),
            JoinType::Unspecified => unreachable!(),
        }
    }
}

/// Try to split and pushdown `predicate` into a into a join condition and into the inputs of the
/// join. Returns the pushed predicates. The pushed part will be removed from the original
/// predicate.
///
/// `InputRef`s in the right pushed condition are indexed by the right child's output schema.

pub fn push_down_into_join(
    predicate: &mut Condition,
    left_col_num: usize,
    right_col_num: usize,
    ty: JoinType,
) -> (Condition, Condition, Condition) {
    let (left, right) = push_down_to_inputs(
        predicate,
        left_col_num,
        right_col_num,
        can_push_left_from_filter(ty),
        can_push_right_from_filter(ty),
    );

    let on = if can_push_on_from_filter(ty) {
        let mut conjunctions = std::mem::take(&mut predicate.conjunctions);

        // Do not push now on to the on, it will be pulled up into a filter instead.
        let on = Condition {
            conjunctions: conjunctions
                .drain_filter(|expr| expr.count_nows() == 0)
                .collect(),
        };
        predicate.conjunctions = conjunctions;
        on
    } else {
        Condition::true_cond()
    };
    (left, right, on)
}

/// Try to pushes parts of the join condition to its inputs. Returns the pushed predicates. The
/// pushed part will be removed from the original join predicate.
///
/// `InputRef`s in the right pushed condition are indexed by the right child's output schema.

pub fn push_down_join_condition(
    on_condition: &mut Condition,
    left_col_num: usize,
    right_col_num: usize,
    ty: JoinType,
) -> (Condition, Condition) {
    push_down_to_inputs(
        on_condition,
        left_col_num,
        right_col_num,
        can_push_left_from_on(ty),
        can_push_right_from_on(ty),
    )
}

/// Try to split and pushdown `predicate` into a join's left/right child.
/// Returns the pushed predicates. The pushed part will be removed from the original predicate.
///
/// `InputRef`s in the right `Condition` are shifted by `-left_col_num`.
fn push_down_to_inputs(
    predicate: &mut Condition,
    left_col_num: usize,
    right_col_num: usize,
    push_left: bool,
    push_right: bool,
) -> (Condition, Condition) {
    let conjunctions = std::mem::take(&mut predicate.conjunctions);

    let (mut left, right, mut others) =
        Condition { conjunctions }.split(left_col_num, right_col_num);

    if !push_left {
        others.conjunctions.extend(left);
        left = Condition::true_cond();
    };

    let right = if push_right {
        let mut mapping = ColIndexMapping::with_shift_offset(
            left_col_num + right_col_num,
            -(left_col_num as isize),
        );
        right.rewrite_expr(&mut mapping)
    } else {
        others.conjunctions.extend(right);
        Condition::true_cond()
    };

    predicate.conjunctions = others.conjunctions;

    (left, right)
}

pub fn can_push_left_from_filter(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
    )
}

pub fn can_push_right_from_filter(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti
    )
}

pub fn can_push_on_from_filter(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    )
}

pub fn can_push_left_from_on(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi
    )
}

pub fn can_push_right_from_on(ty: JoinType) -> bool {
    matches!(
        ty,
        JoinType::Inner | JoinType::LeftOuter | JoinType::RightSemi
    )
}
