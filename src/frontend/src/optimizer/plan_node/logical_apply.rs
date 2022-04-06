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

use fixedbitset::FixedBitSet;
use risingwave_pb::plan::JoinType;

use super::{
    ColPrunable, LogicalJoin, PlanBase, PlanNode, PlanRef, PlanTreeNodeBinary, ToBatch, ToStream,
};
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
                JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
            ),
            "Invalid join type {:?} for LogicalApply",
            join_type
        );
        let ctx = left.ctx();
        let schema = LogicalJoin::derive_schema(left.schema(), right.schema(), join_type);
        let pk_indices = LogicalJoin::derive_pk(
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
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let left_len = self.left.schema().fields.len();

        let mut left_required_cols = FixedBitSet::with_capacity(self.left.schema().fields().len());
        let mut right_required_cols =
            FixedBitSet::with_capacity(self.right.schema().fields().len());
        required_cols.ones().for_each(|i| {
            if i < left_len {
                left_required_cols.insert(i);
            } else {
                right_required_cols.insert(i - left_len);
            }
        });

        LogicalApply::new(
            self.left.prune_col(&left_required_cols),
            self.right.prune_col(&right_required_cols),
            self.join_type,
        )
        .into()
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
