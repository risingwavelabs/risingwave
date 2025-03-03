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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::util::functional::SameOrElseExt;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DeltaExpression, HashJoinNode, PbInequalityPair};

use super::generic::{GenericPlanNode, Join};
use super::stream::prelude::*;
use super::stream_join_common::StreamJoinCommon;
use super::utils::{Distill, childless_record, plan_node_name, watermark_pretty};
use super::{
    ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, StreamDeltaJoin, StreamNode, generic,
};
use crate::expr::{Expr, ExprDisplay, ExprRewriter, ExprVisitor, InequalityInputPair};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::{MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamHashJoin`] implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHashJoin {
    pub base: PlanBase<Stream>,
    core: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// `(do_state_cleaning, InequalityInputPair {key_required_larger, key_required_smaller,
    /// delta_expression})`. View struct `InequalityInputPair` for details.
    inequality_pairs: Vec<(bool, InequalityInputPair)>,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    is_append_only: bool,

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
    pub fn new(core: generic::Join<PlanRef>, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = core.ctx();

        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match core.join_type {
            JoinType::Inner => core.left.append_only() && core.right.append_only(),
            _ => false,
        };

        let dist = StreamJoinCommon::derive_dist(
            core.left.distribution(),
            core.right.distribution(),
            &core,
        );

        let mut inequality_pairs = vec![];
        let mut clean_left_state_conjunction_idx = None;
        let mut clean_right_state_conjunction_idx = None;

        // Reorder `eq_join_predicate` by placing the watermark column at the beginning.
        let mut reorder_idx = vec![];
        for (i, (left_key, right_key)) in eq_join_predicate.eq_indexes().iter().enumerate() {
            if core.left.watermark_columns().contains(*left_key)
                && core.right.watermark_columns().contains(*right_key)
            {
                reorder_idx.push(i);
            }
        }
        let eq_join_predicate = eq_join_predicate.reorder(&reorder_idx);

        let watermark_columns = {
            let l2i = core.l2i_col_mapping();
            let r2i = core.r2i_col_mapping();

            let mut equal_condition_clean_state = false;
            let mut watermark_columns = WatermarkColumns::new();
            for (left_key, right_key) in eq_join_predicate.eq_indexes() {
                if let Some(l_wtmk_group) = core.left.watermark_columns().get_group(left_key)
                    && let Some(r_wtmk_group) = core.right.watermark_columns().get_group(right_key)
                {
                    equal_condition_clean_state = true;
                    if let Some(internal) = l2i.try_map(left_key) {
                        watermark_columns.insert(
                            internal,
                            l_wtmk_group
                                .same_or_else(r_wtmk_group, || ctx.next_watermark_group_id()),
                        );
                    }
                    if let Some(internal) = r2i.try_map(right_key) {
                        watermark_columns.insert(
                            internal,
                            l_wtmk_group
                                .same_or_else(r_wtmk_group, || ctx.next_watermark_group_id()),
                        );
                    }
                }
            }
            let (left_cols_num, original_inequality_pairs) = eq_join_predicate.inequality_pairs();
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
                    core.left.watermark_columns().contains(key_required_larger)
                        && core
                            .right
                            .watermark_columns()
                            .contains(key_required_smaller - left_cols_num)
                } else {
                    core.left.watermark_columns().contains(key_required_smaller)
                        && core
                            .right
                            .watermark_columns()
                            .contains(key_required_larger - left_cols_num)
                };
                if !both_upstream_has_watermark {
                    continue;
                }

                let (internal_col1, internal_col2, do_state_cleaning) =
                    if key_required_larger < key_required_smaller {
                        (
                            l2i.try_map(key_required_larger),
                            r2i.try_map(key_required_smaller - left_cols_num),
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
                            l2i.try_map(key_required_smaller),
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
                if let Some(internal) = internal_col1
                    && !watermark_columns.contains(internal)
                {
                    watermark_columns.insert(internal, ctx.next_watermark_group_id());
                    is_valuable_inequality = true;
                }
                if let Some(internal) = internal_col2
                    && !watermark_columns.contains(internal)
                {
                    watermark_columns.insert(internal, ctx.next_watermark_group_id());
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
            watermark_columns.map_clone(&core.i2o_col_mapping())
        };

        // TODO: derive from input
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            append_only,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );

        Self {
            base,
            core,
            eq_join_predicate,
            inequality_pairs,
            is_append_only: append_only,
            clean_left_state_conjunction_idx,
            clean_right_state_conjunction_idx,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    /// Get a reference to the hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    /// Convert this hash join to a delta join plan
    pub fn into_delta_join(self) -> StreamDeltaJoin {
        StreamDeltaJoin::new(self.core, self.eq_join_predicate)
    }

    pub fn derive_dist_key_in_join_key(&self) -> Vec<usize> {
        let left_dk_indices = self.left().distribution().dist_column_indices().to_vec();
        let right_dk_indices = self.right().distribution().dist_column_indices().to_vec();

        StreamJoinCommon::get_dist_key_in_join_key(
            &left_dk_indices,
            &right_dk_indices,
            self.eq_join_predicate(),
        )
    }

    pub fn inequality_pairs(&self) -> &Vec<(bool, InequalityInputPair)> {
        &self.inequality_pairs
    }
}

impl Distill for StreamHashJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let (ljk, rjk) = self
            .eq_join_predicate
            .eq_indexes()
            .first()
            .cloned()
            .expect("first join key");

        let name = plan_node_name!("StreamHashJoin",
            { "window", self.left().watermark_columns().contains(ljk) && self.right().watermark_columns().contains(rjk) },
            { "interval", self.clean_left_state_conjunction_idx.is_some() && self.clean_right_state_conjunction_idx.is_some() },
            { "append_only", self.is_append_only },
        );
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(6);
        vec.push(("type", Pretty::debug(&self.core.join_type)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        let get_cond = |conjunction_idx| {
            Pretty::debug(&ExprDisplay {
                expr: &self.eq_join_predicate().other_cond().conjunctions[conjunction_idx],
                input_schema: &concat_schema,
            })
        };
        if let Some(i) = self.clean_left_state_conjunction_idx {
            vec.push(("conditions_to_clean_left_state_table", get_cond(i)));
        }
        if let Some(i) = self.clean_right_state_conjunction_idx {
            vec.push(("conditions_to_clean_right_state_table", get_cond(i)));
        }
        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        childless_record(name, vec)
    }
}

impl PlanTreeNodeBinary for StreamHashJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = left;
        core.right = right;
        Self::new(core, self.eq_join_predicate.clone())
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

        let (left_table, left_degree_table, left_deduped_input_pk_indices) =
            Join::infer_internal_and_degree_table_catalog(
                self.left().plan_base(),
                left_jk_indices,
                dk_indices_in_jk.clone(),
            );
        let (right_table, right_degree_table, right_deduped_input_pk_indices) =
            Join::infer_internal_and_degree_table_catalog(
                self.right().plan_base(),
                right_jk_indices,
                dk_indices_in_jk,
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

        NodeBody::HashJoin(Box::new(HashJoinNode {
            join_type: self.core.join_type as i32,
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
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
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            is_append_only: self.is_append_only,
        }))
    }
}

impl ExprRewritable for StreamHashJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.eq_join_predicate.rewrite_exprs(r)).into()
    }
}

impl ExprVisitable for StreamHashJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
