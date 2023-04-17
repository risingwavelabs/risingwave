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

use std::cmp::{max, min};
use std::collections::btree_map::OccupiedError;
use std::collections::BTreeMap;
use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{FieldDisplay, Schema};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DeltaExpression, HashJoinNode, PbBandJoinCondition, PbInequalityPair,
};

use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, PlanTreeNodeUnary,
    StreamDeltaJoin, StreamExchange, StreamNode, StreamProject,
};
use crate::expr::{Expr, ExprDisplay, ExprRewriter, InequalityInputPair};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMappingRewriteExt, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BandCondition {
    left_column_idx: usize,
    right_column_idx: usize,
    left_greater_conjunction_idx: usize,
    right_greater_conjunction_idx: usize,
}

fn find_equivalence_classes(mut input: PlanRef) -> Vec<usize> {
    let len = input.schema().len();
    while let Some(exchange) = input.downcast_ref::<StreamExchange>() {
        input = exchange.input();
    }
    if let Some(project) = input.downcast_ref::<StreamProject>() {
        project
            .exprs()
            .iter()
            .enumerate()
            .map(|(expr_idx, expr)| match expr.recursively_input_offset() {
                Some(input_index) => len + input_index,
                None => expr_idx,
            })
            .collect_vec()
    } else {
        (0..len).collect_vec()
    }
}

/// [`StreamHashJoin`] implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHashJoin {
    pub base: PlanBase,
    logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// `(do_state_cleaning, InequalityInputPair {key_required_larger, key_required_smaller,
    /// delta_expression})`. View struct `InequalityInputPair` for details.
    inequality_pairs: Vec<(bool, InequalityInputPair)>,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    is_append_only: bool,

    /// A pair of band conditions. We can select entries which match the condition only in
    /// `HashJoinExecutor`.
    band_condition: Option<BandCondition>,

    /// The conjunction index of the inequality which is used to clean left state table in
    /// `HashJoinExecutor`. If any equal condition is able to clean state table, this field
    /// will always be `None`.
    clean_left_state_conjunction_idx: Option<usize>,
    /// The conjunction index of the inequality which is used to clean right state table in
    /// `HashJoinExecutor`. If any equal condition is able to clean state table, this field
    /// will always be `None`.
    clean_right_state_conjunction_idx: Option<usize>,
}

impl StreamHashJoin {
    pub fn new(logical: generic::Join<PlanRef>, eq_join_predicate: EqJoinPredicate) -> Self {
        let base = PlanBase::new_logical_with_core(&logical);
        let ctx = base.ctx;
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match logical.join_type {
            JoinType::Inner => logical.left.append_only() && logical.right.append_only(),
            _ => false,
        };

        let dist = Self::derive_dist(
            logical.left.distribution(),
            logical.right.distribution(),
            &logical,
        );

        let mut band_condition = None;

        let mut inequality_pairs = vec![];
        let mut clean_left_state_conjunction_idx = None;
        let mut clean_right_state_conjunction_idx = None;

        // Reorder `eq_join_predicate` by placing the watermark column at the beginning.
        let mut reorder_idx = vec![];
        for (i, (left_key, right_key)) in eq_join_predicate.eq_indexes().iter().enumerate() {
            if logical.left.watermark_columns().contains(*left_key)
                && logical.right.watermark_columns().contains(*right_key)
            {
                reorder_idx.push(i);
            }
        }
        let eq_join_predicate = eq_join_predicate.reorder(&reorder_idx);

        let watermark_columns = {
            let l2i = logical.l2i_col_mapping();
            let r2i = logical.r2i_col_mapping();
            let (left_cols_num, original_inequality_pairs) = eq_join_predicate.inequality_pairs();
            let left_equivalence_classes = find_equivalence_classes(logical.left.clone());
            let right_equivalence_classes = find_equivalence_classes(logical.right.clone());

            let mut equal_condition_clean_state = false;
            let mut left_join_key_classes =
                Vec::with_capacity(eq_join_predicate.eq_indexes().len());
            let mut right_join_key_classes =
                Vec::with_capacity(eq_join_predicate.eq_indexes().len());
            let mut watermark_columns = FixedBitSet::with_capacity(logical.internal_column_num());
            for (left_key, right_key) in eq_join_predicate.eq_indexes() {
                left_join_key_classes.push(left_equivalence_classes[left_key]);
                right_join_key_classes.push(right_equivalence_classes[right_key]);
                if logical.left.watermark_columns().contains(left_key)
                    && logical.right.watermark_columns().contains(right_key)
                {
                    equal_condition_clean_state = true;
                    if let Some(internal) = l2i.try_map(left_key) {
                        watermark_columns.insert(internal);
                    }
                    if let Some(internal) = r2i.try_map(right_key) {
                        watermark_columns.insert(internal);
                    }
                }
            }
            left_join_key_classes.sort();
            left_join_key_classes.dedup();
            right_join_key_classes.sort();
            right_join_key_classes.dedup();

            let mut both_pk_in_jk = true;
            for left_pk in logical.left.logical_pk() {
                if left_join_key_classes
                    .binary_search(&left_equivalence_classes[*left_pk])
                    .is_err()
                {
                    both_pk_in_jk = false;
                    break;
                }
            }
            if both_pk_in_jk {
                for right_pk in logical.right.logical_pk() {
                    if right_join_key_classes
                        .binary_search(&right_equivalence_classes[*right_pk])
                        .is_err()
                    {
                        both_pk_in_jk = false;
                        break;
                    }
                }
            }

            if !both_pk_in_jk {
                let mut pair2conjunction_idx = BTreeMap::new();
                for (
                    conjunction_idx,
                    InequalityInputPair {
                        key_required_larger,
                        key_required_smaller,
                        ..
                    },
                ) in &original_inequality_pairs
                {
                    let left_class =
                        left_equivalence_classes[min(*key_required_larger, *key_required_smaller)];
                    let right_class = right_equivalence_classes
                        [max(key_required_larger, key_required_smaller) - left_cols_num];

                    if left_join_key_classes.binary_search(&left_class).is_err()
                        && right_join_key_classes.binary_search(&right_class).is_err()
                    {
                        if let Err(OccupiedError { entry, .. }) = pair2conjunction_idx.try_insert(
                            (left_class, right_class),
                            (
                                *conjunction_idx,
                                *key_required_larger,
                                *key_required_smaller,
                            ),
                        ) {
                            let former = entry.get();
                            if former.1 < former.2 {
                                if key_required_larger > key_required_smaller {
                                    band_condition = Some(BandCondition {
                                        left_column_idx: former.1,
                                        right_column_idx: former.2 - left_cols_num,
                                        left_greater_conjunction_idx: former.0,
                                        right_greater_conjunction_idx: *conjunction_idx,
                                    });
                                    break;
                                }
                            } else {
                                // former.1 > former.2
                                if key_required_larger < key_required_smaller {
                                    band_condition = Some(BandCondition {
                                        left_column_idx: former.2,
                                        right_column_idx: former.1 - left_cols_num,
                                        left_greater_conjunction_idx: *conjunction_idx,
                                        right_greater_conjunction_idx: former.0,
                                    });
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            for (
                conjunction_idx,
                InequalityInputPair {
                    key_required_larger,
                    key_required_smaller,
                    delta_expression,
                },
            ) in original_inequality_pairs
            {
                let both_upstream_has_watermark = if key_required_larger < key_required_smaller {
                    logical
                        .left
                        .watermark_columns()
                        .contains(key_required_larger)
                        && logical
                            .right
                            .watermark_columns()
                            .contains(key_required_smaller - left_cols_num)
                } else {
                    logical
                        .left
                        .watermark_columns()
                        .contains(key_required_smaller)
                        && logical
                            .right
                            .watermark_columns()
                            .contains(key_required_larger - left_cols_num)
                };
                if !both_upstream_has_watermark {
                    continue;
                }

                let (internal, do_state_cleaning) = if key_required_larger < key_required_smaller {
                    (
                        l2i.try_map(key_required_larger),
                        if !equal_condition_clean_state
                            && clean_left_state_conjunction_idx.is_none()
                        {
                            clean_left_state_conjunction_idx = Some(conjunction_idx);
                            true
                        } else {
                            false
                        },
                    )
                } else {
                    (
                        r2i.try_map(key_required_larger - left_cols_num),
                        if !equal_condition_clean_state
                            && clean_right_state_conjunction_idx.is_none()
                        {
                            clean_right_state_conjunction_idx = Some(conjunction_idx);
                            true
                        } else {
                            false
                        },
                    )
                };
                let mut is_valuable_inequality = do_state_cleaning;
                if let Some(internal) = internal && !watermark_columns.contains(internal) {
                    watermark_columns.insert(internal);
                    is_valuable_inequality = true;
                }
                if is_valuable_inequality {
                    inequality_pairs.push((
                        do_state_cleaning,
                        InequalityInputPair {
                            key_required_larger,
                            key_required_smaller,
                            delta_expression,
                        },
                    ));
                }
            }
            logical.i2o_col_mapping().rewrite_bitset(&watermark_columns)
        };

        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            base.schema,
            base.logical_pk,
            base.functional_dependency,
            dist,
            append_only,
            watermark_columns,
        );

        Self {
            base,
            logical,
            eq_join_predicate,
            inequality_pairs,
            is_append_only: append_only,
            band_condition,
            clean_left_state_conjunction_idx,
            clean_right_state_conjunction_idx,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.logical.join_type
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        logical: &generic::Join<PlanRef>,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => {
                // we can not derive the hash distribution from the side where outer join can
                // generate a NULL row
                match logical.join_type {
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
    pub fn into_delta_join(self) -> StreamDeltaJoin {
        StreamDeltaJoin::new(self.logical, self.eq_join_predicate)
    }

    pub fn derive_dist_key_in_join_key(&self) -> Vec<usize> {
        let left_dk_indices = self.left().distribution().dist_column_indices().to_vec();
        let right_dk_indices = self.right().distribution().dist_column_indices().to_vec();
        let left_jk_indices = self.eq_join_predicate.left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate.right_eq_indexes();

        assert_eq!(left_jk_indices.len(), right_jk_indices.len());

        let mut dk_indices_in_jk = vec![];

        for (l_dk_idx, r_dk_idx) in left_dk_indices.iter().zip_eq_fast(right_dk_indices.iter()) {
            for dk_idx_in_jk in left_jk_indices.iter().positions(|idx| idx == l_dk_idx) {
                if right_jk_indices[dk_idx_in_jk] == *r_dk_idx {
                    dk_indices_in_jk.push(dk_idx_in_jk);
                    break;
                }
            }
        }

        assert_eq!(dk_indices_in_jk.len(), left_dk_indices.len());
        dk_indices_in_jk
    }

    pub fn inequality_pairs(&self) -> &Vec<(bool, InequalityInputPair)> {
        &self.inequality_pairs
    }

    pub fn band_condition(&self) -> &Option<BandCondition> {
        &self.band_condition
    }
}

impl fmt::Display for StreamHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (ljk, rjk) = self
            .eq_join_predicate
            .eq_indexes()
            .first()
            .cloned()
            .expect("first join key");

        let mut builder = if self.left().watermark_columns().contains(ljk)
            && self.right().watermark_columns().contains(rjk)
        {
            f.debug_struct("StreamWindowJoin")
        } else if self.band_condition.is_some() {
            f.debug_struct("StreamIntervalJoin")
        } else if self.is_append_only {
            f.debug_struct("StreamAppendOnlyHashJoin")
        } else {
            f.debug_struct("StreamHashJoin")
        };

        let verbose = self.base.ctx.is_explain_verbose();
        builder.field("type", &self.logical.join_type);

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

        if let Some(band_condition) = self.band_condition.as_ref() {
            builder.field(
                "band_join_condition_left_greater",
                &ExprDisplay {
                    expr: &self.eq_join_predicate().other_cond().conjunctions
                        [band_condition.left_greater_conjunction_idx],
                    input_schema: &concat_schema,
                },
            );
            builder.field(
                "band_join_condition_right_greater",
                &ExprDisplay {
                    expr: &self.eq_join_predicate().other_cond().conjunctions
                        [band_condition.right_greater_conjunction_idx],
                    input_schema: &concat_schema,
                },
            );
        }

        if let Some(conjunction_idx) = self.clean_left_state_conjunction_idx {
            builder.field(
                "conditions_to_clean_left_state_table",
                &ExprDisplay {
                    expr: &self.eq_join_predicate().other_cond().conjunctions[conjunction_idx],
                    input_schema: &concat_schema,
                },
            );
        }
        if let Some(conjunction_idx) = self.clean_right_state_conjunction_idx {
            builder.field(
                "conditions_to_clean_right_state_table",
                &ExprDisplay {
                    expr: &self.eq_join_predicate().other_cond().conjunctions[conjunction_idx],
                    input_schema: &concat_schema,
                },
            );
        }

        let watermark_columns = &self.base.watermark_columns;
        if self.base.watermark_columns.count_ones(..) > 0 {
            let schema = self.schema();
            builder.field(
                "output_watermarks",
                &watermark_columns
                    .ones()
                    .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
                    .collect_vec(),
            );
        };

        if verbose {
            if self
                .logical
                .output_indices
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: &self.logical.output_indices,
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

impl_plan_tree_node_for_binary! { StreamHashJoin }

impl StreamNode for StreamHashJoin {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let left_jk_indices = self.eq_join_predicate.left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate.right_eq_indexes();
        let left_jk_indices_prost = left_jk_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_jk_indices_prost = right_jk_indices.iter().map(|idx| *idx as i32).collect_vec();

        let dk_indices_in_jk = self.derive_dist_key_in_join_key();

        use super::stream::HashJoin;
        let (left_table, left_degree_table, left_deduped_input_pk_indices) =
            HashJoin::infer_internal_and_degree_table_catalog(
                self.left().plan_base(),
                left_jk_indices,
                dk_indices_in_jk.clone(),
                self.band_condition
                    .as_ref()
                    .map(|band_condition| band_condition.left_column_idx),
            );
        let (right_table, right_degree_table, right_deduped_input_pk_indices) =
            HashJoin::infer_internal_and_degree_table_catalog(
                self.right().plan_base(),
                right_jk_indices,
                dk_indices_in_jk,
                self.band_condition
                    .as_ref()
                    .map(|band_condition| band_condition.right_column_idx),
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

        let condition = if let Some(band_condition) = self.band_condition.as_ref() {
            Condition {
                conjunctions: self
                    .eq_join_predicate
                    .other_cond()
                    .conjunctions
                    .iter()
                    .enumerate()
                    .filter(|(conjunction_idx, _)| {
                        *conjunction_idx != band_condition.left_greater_conjunction_idx
                            && *conjunction_idx != band_condition.right_greater_conjunction_idx
                    })
                    .map(|(_, conjunction)| conjunction.clone())
                    .collect_vec(),
            }
            .as_expr_unless_true()
        } else {
            self.eq_join_predicate.other_cond().as_expr_unless_true()
        };

        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type as i32,
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            condition: condition.map(|x| x.to_expr_proto()),
            inequality_pairs: self
                .inequality_pairs
                .iter()
                .map(
                    |(
                        do_state_clean,
                        InequalityInputPair {
                            key_required_larger,
                            key_required_smaller,
                            delta_expression,
                        },
                    )| {
                        PbInequalityPair {
                            key_required_larger: *key_required_larger as u32,
                            key_required_smaller: *key_required_smaller as u32,
                            clean_state: *do_state_clean,
                            delta_expression: delta_expression.as_ref().map(
                                |(delta_type, delta)| DeltaExpression {
                                    delta_type: *delta_type as i32,
                                    delta: Some(delta.to_expr_proto()),
                                },
                            ),
                        }
                    },
                )
                .collect_vec(),
            left_table: Some(left_table.to_internal_table_prost()),
            right_table: Some(right_table.to_internal_table_prost()),
            left_degree_table: Some(left_degree_table.to_internal_table_prost()),
            right_degree_table: Some(right_degree_table.to_internal_table_prost()),
            left_deduped_input_pk_indices,
            right_deduped_input_pk_indices,
            output_indices: self
                .logical
                .output_indices
                .iter()
                .map(|&x| x as u32)
                .collect(),
            is_append_only: self.is_append_only,
            band_condition: self.band_condition.as_ref().map(|band_condition| {
                PbBandJoinCondition {
                    left_column_idx: band_condition.left_column_idx as u32,
                    right_column_idx: band_condition.right_column_idx as u32,
                    left_greater_conjunction: Some(
                        self.eq_join_predicate.other_cond().conjunctions
                            [band_condition.left_greater_conjunction_idx]
                            .to_expr_proto(),
                    ),
                    right_greater_conjunction: Some(
                        self.eq_join_predicate.other_cond().conjunctions
                            [band_condition.right_greater_conjunction_idx]
                            .to_expr_proto(),
                    ),
                }
            }),
        })
    }
}

impl ExprRewritable for StreamHashJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical, self.eq_join_predicate.rewrite_exprs(r)).into()
    }
}
