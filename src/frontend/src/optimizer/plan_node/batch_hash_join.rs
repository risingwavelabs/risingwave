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

use std::fmt;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashJoinNode;
use risingwave_pb::plan_common::JoinType;

use super::generic::{self, GenericPlanRef};
use super::utils::{childless_record, Distill};
use super::{
    EqJoinPredicate, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, ToBatchPb,
    ToDistributedBatch,
};
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicateDisplay, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::utils::ColIndexMappingRewriteExt;

/// `BatchHashJoin` implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and then probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchHashJoin {
    pub base: PlanBase,
    logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl BatchHashJoin {
    pub fn new(logical: generic::Join<PlanRef>, eq_join_predicate: EqJoinPredicate) -> Self {
        let dist = Self::derive_dist(
            logical.left.distribution(),
            logical.right.distribution(),
            &logical,
        );
        let base = PlanBase::new_batch_from_logical(&logical, dist, Order::any());

        Self {
            base,
            logical,
            eq_join_predicate,
        }
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        logical: &generic::Join<PlanRef>,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            // we can not derive the hash distribution from the side where outer join can generate a
            // NULL row
            (Distribution::HashShard(_), Distribution::HashShard(_)) => match logical.join_type {
                JoinType::Unspecified => unreachable!(),
                JoinType::FullOuter => Distribution::SomeShard,
                JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => {
                    let l2o = logical
                        .l2i_col_mapping()
                        .composite(&logical.i2o_col_mapping());
                    l2o.rewrite_provided_distribution(left)
                }
                JoinType::RightSemi | JoinType::RightAnti | JoinType::RightOuter => {
                    let r2o = logical
                        .r2i_col_mapping()
                        .composite(&logical.i2o_col_mapping());
                    r2o.rewrite_provided_distribution(right)
                }
            },
            (_, _) => unreachable!(
                "suspicious distribution: left: {:?}, right: {:?}",
                left, right
            ),
        }
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl Distill for BatchHashJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.logical.join_type)));

        let concat_schema = self.logical.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));
        if verbose {
            let data = IndicesDisplay::from_join(&self.logical, &concat_schema)
                .map_or_else(|| Pretty::from("all"), |id| Pretty::display(&id));
            vec.push(("output", data));
        }
        childless_record("BatchHashJoin", vec)
    }
}

impl fmt::Display for BatchHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("BatchHashJoin");
        builder.field("type", &self.logical.join_type);

        let concat_schema = self.logical.concat_schema();
        builder.field(
            "predicate",
            &EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            },
        );

        if verbose {
            match IndicesDisplay::from_join(&self.logical, &concat_schema) {
                None => builder.field("output", &format_args!("all")),
                Some(id) => builder.field("output", &id),
            };
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for BatchHashJoin {
    fn left(&self) -> PlanRef {
        self.logical.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.logical.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.left = left;
        logical.right = right;
        Self::new(logical, self.eq_join_predicate.clone())
    }
}

impl_plan_tree_node_for_binary! { BatchHashJoin }

impl ToDistributedBatch for BatchHashJoin {
    fn to_distributed(&self) -> Result<PlanRef> {
        let mut right = self.right().to_distributed_with_required(
            &Order::any(),
            &RequiredDist::shard_by_key(
                self.right().schema().len(),
                &self.eq_join_predicate().right_eq_indexes(),
            ),
        )?;
        let mut left = self.left();

        let r2l = self
            .eq_join_predicate()
            .r2l_eq_columns_mapping(left.schema().len(), right.schema().len());
        let l2r = self
            .eq_join_predicate()
            .l2r_eq_columns_mapping(left.schema().len());

        let right_dist = right.distribution();
        match right_dist {
            Distribution::HashShard(_) => {
                let left_dist = r2l
                    .rewrite_required_distribution(&RequiredDist::PhysicalDist(right_dist.clone()));
                left = left.to_distributed_with_required(&Order::any(), &left_dist)?;
            }
            Distribution::UpstreamHashShard(_, _) => {
                left = left.to_distributed_with_required(
                    &Order::any(),
                    &RequiredDist::shard_by_key(
                        self.left().schema().len(),
                        &self.eq_join_predicate().left_eq_indexes(),
                    ),
                )?;
                let left_dist = left.distribution();
                match left_dist {
                    Distribution::HashShard(_) => {
                        let right_dist = l2r.rewrite_required_distribution(
                            &RequiredDist::PhysicalDist(left_dist.clone()),
                        );
                        right = right_dist.enforce_if_not_satisfies(right, &Order::any())?
                    }
                    Distribution::UpstreamHashShard(_, _) => {
                        left =
                            RequiredDist::hash_shard(&self.eq_join_predicate().left_eq_indexes())
                                .enforce_if_not_satisfies(left, &Order::any())?;
                        right =
                            RequiredDist::hash_shard(&self.eq_join_predicate().right_eq_indexes())
                                .enforce_if_not_satisfies(right, &Order::any())?;
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }

        Ok(self.clone_with_left_right(left, right).into())
    }
}

impl ToBatchPb for BatchHashJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type as i32,
            left_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            right_key: self
                .eq_join_predicate
                .right_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            null_safe: self.eq_join_predicate.null_safes().into_iter().collect(),
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            output_indices: self
                .logical
                .output_indices
                .iter()
                .map(|&x| x as u32)
                .collect(),
        })
    }
}

impl ToLocalBatch for BatchHashJoin {
    fn to_local(&self) -> Result<PlanRef> {
        let right = RequiredDist::single()
            .enforce_if_not_satisfies(self.right().to_local()?, &Order::any())?;
        let left = RequiredDist::single()
            .enforce_if_not_satisfies(self.left().to_local()?, &Order::any())?;

        Ok(self.clone_with_left_right(left, right).into())
    }
}

impl ExprRewritable for BatchHashJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical, self.eq_join_predicate.rewrite_exprs(r)).into()
    }
}
