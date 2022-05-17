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

use risingwave_common::error::Result;
use risingwave_pb::plan_common::JoinType;

use super::{ColPrunable, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatch, ToStream};
use crate::optimizer::plan_node::PlanTreeNode;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalMultiJoin` combines two or more relations according to some condition.
///
/// Each output row has fields from one the inputs. The set of output rows is a subset
/// of the cartesian product of all the inputs; The `LogicalMultiInnerJoin` is only supported
/// for inner joins as it implicitly assumes commutativity. Non-inner joins should be
/// expressed as 2-way `LogicalJoin`s.
#[derive(Debug, Clone)]
pub struct LogicalMultiJoin {
    pub base: PlanBase,
    inputs: Vec<PlanRef>,
    on: Condition,
}

impl fmt::Display for LogicalMultiJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogicalMultiJoin {{ on: {} }}", &self.on)
    }
}

impl LogicalMultiJoin {
    pub(crate) fn new(base: PlanBase, inputs: Vec<PlanRef>, on: Condition) -> Self {
        Self { base, inputs, on }
    }

    pub(crate) fn from_join(join: &PlanRef) -> Option<Self> {
        let logical_join = join.as_logical_join()?;
        if logical_join.join_type() != JoinType::Inner {
            return None;
        }
        let left = logical_join.left();
        let right = logical_join.right();

        let left_col_num = left.schema().len();
        let right_col_num = right.schema().len();

        let mut inputs = vec![];
        let mut conjunctions = logical_join.on().conjunctions.clone();
        if let Some(multi_join) = left.as_logical_multi_join() {
            inputs.extend(multi_join.inputs());
            conjunctions.extend(multi_join.on().clone());
        } else {
            inputs.push(left.clone());
        }
        if let Some(multi_join) = right.as_logical_multi_join() {
            inputs.extend(multi_join.inputs());
            let right_on = multi_join.on().clone();
            let mut mapping =
                ColIndexMapping::with_shift_offset(right_col_num, left_col_num as isize);
            let new_on = right_on.rewrite_expr(&mut mapping);
            conjunctions.extend(new_on.conjunctions);
        } else {
            inputs.push(right.clone());
        }

        Some(Self {
            base: logical_join.base.clone(),
            inputs,
            on: Condition { conjunctions },
        })
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new(self.base.clone(), self.inputs.clone(), cond)
    }
}

impl PlanTreeNode for LogicalMultiJoin {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(
        &self,
        _inputs: &[crate::optimizer::PlanRef],
    ) -> crate::optimizer::PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ToStream for LogicalMultiJoin {
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }

    fn to_stream(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ToBatch for LogicalMultiJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}

impl ColPrunable for LogicalMultiJoin {
    fn prune_col(&self, _required_cols: &[usize]) -> PlanRef {
        panic!(
            "Method not available for `LogicalMultiJoin` which is a placeholder node with \
             a temporary lifetime. It only facilitates join reordering during logical planning."
        )
    }
}
