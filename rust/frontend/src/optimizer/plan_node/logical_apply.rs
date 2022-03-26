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
use std::{fmt, iter};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_pb::plan::JoinType;

use super::{ColPrunable, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatch, ToStream};
use crate::utils::ColIndexMapping;

/// `LogicalApply` represents a correlated join, where the right side may refer to columns from the
/// left side.
#[derive(Debug, Clone)]
pub struct LogicalApply {
    pub base: PlanBase,
    left: PlanRef,
    right: PlanRef,
    join_type: JoinType,
}

impl fmt::Display for LogicalApply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogicalApply {{ type: {:?} }}", &self.join_type)
    }
}

impl LogicalApply {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType) -> Self {
        assert!(
            matches!(
                join_type,
                JoinType::FullOuter | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
            ),
            "Invalid join type {:?} for LogicalApply",
            join_type
        );
        let ctx = left.ctx();
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type);
        let pk_indices = Self::derive_pk(
            left.schema().len(),
            right.schema().len(),
            left.pk_indices(),
            right.pk_indices(),
            join_type,
        );
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalApply {
            base,
            left,
            right,
            join_type,
        }
    }

    pub fn create(left: PlanRef, right: PlanRef, join_type: JoinType) -> PlanRef {
        Self::new(left, right, join_type).into()
    }
    pub fn out_column_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            _ => unimplemented!(),
        }
    }

    /// get the Mapping of columnIndex from output column index to left column index
    pub fn o2l_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::new(
                    (0..left_len)
                        .into_iter()
                        .map(Some)
                        .chain(iter::repeat(None).take(right_len))
                        .collect_vec(),
                )
            }
            _ => unimplemented!(),
        }
    }

    /// get the Mapping of columnIndex from output column index to right column index
    pub fn o2r_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            _ => unimplemented!(),
        }
    }

    /// get the Mapping of columnIndex from left column index to output column index
    pub fn l2o_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::o2l_col_mapping(left_len, right_len, join_type).inverse()
    }

    /// get the Mapping of columnIndex from right column index to output column index
    pub fn r2o_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::o2r_col_mapping(left_len, right_len, join_type).inverse()
    }

    fn derive_schema(left_schema: &Schema, right_schema: &Schema, join_type: JoinType) -> Schema {
        let left_len = left_schema.len();
        let right_len = right_schema.len();
        let out_column_num = Self::out_column_num(left_len, right_len, join_type);
        let o2l = Self::o2l_col_mapping(left_len, right_len, join_type);
        let o2r = Self::o2r_col_mapping(left_len, right_len, join_type);
        let fields = (0..out_column_num)
            .into_iter()
            .map(|i| match (o2l.try_map(i), o2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(),
            })
            .collect();
        Schema { fields }
    }

    fn derive_pk(
        left_len: usize,
        right_len: usize,
        left_pk: &[usize],
        right_pk: &[usize],
        join_type: JoinType,
    ) -> Vec<usize> {
        let l2o = Self::l2o_col_mapping(left_len, right_len, join_type);
        let r2o = Self::r2o_col_mapping(left_len, right_len, join_type);
        left_pk
            .iter()
            .map(|index| l2o.map(*index))
            .chain(right_pk.iter().map(|index| r2o.map(*index)))
            .collect()
    }

    /// Get the join type of the logical apply.
    pub fn join_type(&self) -> JoinType {
        self.join_type
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
        Self::new(left, right, self.join_type)
    }
}

impl_plan_tree_node_for_binary! { LogicalApply }

impl ColPrunable for LogicalApply {
    fn prune_col(&self, _: &FixedBitSet) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl ToBatch for LogicalApply {
    fn to_batch(&self) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl ToStream for LogicalApply {
    fn to_stream(&self) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }

    fn logical_rewrite_for_stream(&self) -> (PlanRef, ColIndexMapping) {
        panic!("LogicalApply should be unnested")
    }
}
