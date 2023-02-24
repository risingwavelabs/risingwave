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
use std::ops::BitAnd;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::HashJoinNode;

use super::{
    ExprRewritable, LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, StreamDeltaJoin, StreamNode,
};
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// [`StreamHashJoin`] implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHashJoin {
    pub base: PlanBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    is_append_only: bool,
}

impl StreamHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match logical.join_type() {
            JoinType::Inner => logical.left().append_only() && logical.right().append_only(),
            _ => false,
        };

        let dist = Self::derive_dist(
            logical.left().distribution(),
            logical.right().distribution(),
            &logical,
        );
        let watermark_columns = {
            let from_left = logical
                .l2i_col_mapping()
                .rewrite_bitset(logical.left().watermark_columns());

            let from_right = logical
                .r2i_col_mapping()
                .rewrite_bitset(logical.right().watermark_columns());
            let watermark_columns = from_left.bitand(&from_right);
            logical.i2o_col_mapping().rewrite_bitset(&watermark_columns)
        };

        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.logical_pk.to_vec(),
            logical.functional_dependency().clone(),
            dist,
            append_only,
            watermark_columns,
        );

        Self {
            base,
            logical,
            eq_join_predicate,
            is_append_only: append_only,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.logical.join_type()
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        logical: &LogicalJoin,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                // we can not derive the hash distribution from the side where outer join can
                // generate a NULL row
                match logical.join_type() {
                    JoinType::Unspecified => unreachable!(),
                    JoinType::FullOuter => Distribution::SomeShard,
                    JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti => {
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
                }
            }
            (_, _) => unreachable!(
                "suspicious distribution: left: {:?}, right: {:?}",
                left, right
            ),
        }
    }

    /// Convert this hash join to a delta join plan
    pub fn to_delta_join(&self) -> StreamDeltaJoin {
        StreamDeltaJoin::new(self.logical.clone(), self.eq_join_predicate.clone())
    }
}

impl fmt::Display for StreamHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = if self.is_append_only {
            f.debug_struct("StreamAppendOnlyHashJoin")
        } else {
            f.debug_struct("StreamHashJoin")
        };

        let verbose = self.base.ctx.is_explain_verbose();
        builder.field("type", &self.logical.join_type());

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
        builder.field(
            "predicate",
            &EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            },
        );

        if verbose {
            if self
                .logical
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: self.logical.output_indices(),
                        input_schema: &concat_schema,
                    },
                );
            }
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for StreamHashJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }

    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.eq_join_predicate.clone(),
        )
    }
}

impl_plan_tree_node_for_binary! { StreamHashJoin }

impl StreamNode for StreamHashJoin {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let left_key_indices = self.eq_join_predicate.left_eq_indexes();
        let right_key_indices = self.eq_join_predicate.right_eq_indexes();
        let left_key_indices_prost = left_key_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_key_indices_prost = right_key_indices
            .iter()
            .map(|idx| *idx as i32)
            .collect_vec();

        use super::stream::HashJoin;
        let (left_table, left_degree_table, left_deduped_input_pk_indices) =
            HashJoin::infer_internal_and_degree_table_catalog(
                self.left().plan_base(),
                left_key_indices,
            );
        let (right_table, right_degree_table, right_deduped_input_pk_indices) =
            HashJoin::infer_internal_and_degree_table_catalog(
                self.right().plan_base(),
                right_key_indices,
            );

        let left_deduped_input_pk_indices = left_deduped_input_pk_indices
            .iter()
            .map(|idx| *idx as u32)
            .collect_vec();

        let right_deduped_input_pk_indices = right_deduped_input_pk_indices
            .iter()
            .map(|idx| *idx as u32)
            .collect_vec();

        let (left_table, left_degree_table) = (
            left_table.with_id(state.gen_table_id_wrapped()),
            left_degree_table.with_id(state.gen_table_id_wrapped()),
        );
        let (right_table, right_degree_table) = (
            right_table.with_id(state.gen_table_id_wrapped()),
            right_degree_table.with_id(state.gen_table_id_wrapped()),
        );

        let null_safe_prost = self.eq_join_predicate.null_safes().into_iter().collect();

        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type() as i32,
            left_key: left_key_indices_prost,
            right_key: right_key_indices_prost,
            null_safe: null_safe_prost,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            left_table: Some(left_table.to_internal_table_prost()),
            right_table: Some(right_table.to_internal_table_prost()),
            left_degree_table: Some(left_degree_table.to_internal_table_prost()),
            right_degree_table: Some(right_degree_table.to_internal_table_prost()),
            left_deduped_input_pk_indices,
            right_deduped_input_pk_indices,
            output_indices: self
                .logical
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
            is_append_only: self.is_append_only,
        })
    }
}

impl ExprRewritable for StreamHashJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_join()
                .unwrap()
                .clone(),
            self.eq_join_predicate.rewrite_exprs(r),
        )
        .into()
    }
}
