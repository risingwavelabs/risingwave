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

use risingwave_common::catalog::TableDesc;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::LookupJoinNode;

use crate::expr::Expr;
use crate::optimizer::plan_node::{
    EqJoinPredicate, LogicalJoin, PlanBase, PlanTreeNodeBinary, ToBatchProst, ToDistributedBatch,
    ToLocalBatch,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::PlanRef;

#[derive(Debug, Clone)]
pub struct BatchLookupJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    table_desc: TableDesc,
    right_column_ids: Vec<i32>,
}

impl BatchLookupJoin {
    pub fn new(
        logical: LogicalJoin,
        eq_join_predicate: EqJoinPredicate,
        table_desc: TableDesc,
        right_column_ids: Vec<i32>,
    ) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
        );
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());
        Self {
            base,
            logical,
            eq_join_predicate,
            table_desc,
            right_column_ids,
        }
    }

    fn derive_dist(left: &Distribution, right: &Distribution) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (_, _) => unreachable!(),
        }
    }

    fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for BatchLookupJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BatchLookupJoin {{ type: {:?}, predicate: {}, output_indices: {} }}",
            self.logical.join_type(),
            self.eq_join_predicate(),
            if self
                .logical
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                "all".to_string()
            } else {
                format!("{:?}", self.logical.output_indices())
            }
        )
    }
}

impl PlanTreeNodeBinary for BatchLookupJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }

    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    // Only change left side
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.eq_join_predicate.clone(),
            self.table_desc.clone(),
            self.right_column_ids.clone(),
        )
    }
}

impl_plan_tree_node_for_binary! { BatchLookupJoin }

impl ToDistributedBatch for BatchLookupJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented("Lookup Join in MPP mode".to_string(), None.into()).into())
    }
}

impl ToBatchProst for BatchLookupJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::LookupJoin(LookupJoinNode {
            join_type: self.logical.join_type() as i32,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            build_side_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            probe_side_table_desc: Some(self.table_desc.to_protobuf()),
            output_indices: self
                .logical
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
            sources: vec![],
        })
    }
}

impl ToLocalBatch for BatchLookupJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let left = RequiredDist::single()
            .enforce_if_not_satisfies(self.left().to_local()?, &Order::any())?;
        let right = RequiredDist::single()
            .enforce_if_not_satisfies(self.right().to_local()?, &Order::any())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}
