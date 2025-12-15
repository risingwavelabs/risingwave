// Copyright 2022 RisingWave Labs
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
use risingwave_pb::stream_plan::{
    HashJoinNode, InequalityPairV2 as PbInequalityPairV2, InequalityType as PbInequalityType,
    PbJoinEncodingType,
};

use super::generic::{GenericPlanNode, Join};
use super::stream::prelude::*;
use super::stream_join_common::StreamJoinCommon;
use super::utils::{Distill, childless_record, plan_node_name, watermark_pretty};
use super::{
    ExprRewritable, PlanBase, PlanTreeNodeBinary, StreamDeltaJoin, StreamPlanRef as PlanRef,
    TryToStreamPb, generic,
};
use crate::expr::{Expr, ExprDisplay, ExprRewriter, ExprType, ExprVisitor, InequalityInputPairV2};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::{MonotonicityMap, WatermarkColumns};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamHashJoin`] implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamHashJoin {
    pub base: PlanBase<Stream>,
    core: generic::Join<PlanRef>,

    /// `(clean_left_state, clean_right_state, InequalityInputPairV2)`.
    /// Each entry represents an inequality condition like `left_col <op> right_col`.
    inequality_pairs: Vec<(bool, bool, InequalityInputPairV2)>,

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
    pub fn new(mut core: generic::Join<PlanRef>) -> Result<Self> {
        let ctx = core.ctx();

        let stream_kind = core.stream_kind()?;

        // Reorder `eq_join_predicate` by placing the watermark column at the beginning.
        let eq_join_predicate = {
            let eq_join_predicate = core
                .on
                .as_eq_predicate_ref()
                .expect("StreamHashJoin requires JoinOn::EqPredicate in core")
                .clone();
            let mut reorder_idx = vec![];
            for (i, (left_key, right_key)) in eq_join_predicate.eq_indexes().iter().enumerate() {
                if core.left.watermark_columns().contains(*left_key)
                    && core.right.watermark_columns().contains(*right_key)
                {
                    reorder_idx.push(i);
                }
            }
            eq_join_predicate.reorder(&reorder_idx)
        };
        core.on = generic::JoinOn::EqPredicate(eq_join_predicate.clone());

        let dist = StreamJoinCommon::derive_dist(
            core.left.distribution(),
            core.right.distribution(),
            &core,
        );

        let mut inequality_pairs = vec![];
        let mut clean_left_state_conjunction_idx = None;
        let mut clean_right_state_conjunction_idx = None;

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

            // Process inequality pairs using the new V2 format
            let original_inequality_pairs = eq_join_predicate.inequality_pairs_v2();
            for (conjunction_idx, pair) in original_inequality_pairs {
                let InequalityInputPairV2 {
                    left_idx,
                    right_idx,
                    op,
                } = pair;

                // Check if both upstream sides have watermarks on the inequality columns
                let both_upstream_has_watermark = core.left.watermark_columns().contains(left_idx)
                    && core.right.watermark_columns().contains(right_idx);
                if !both_upstream_has_watermark {
                    continue;
                }

                // Determine which side's state can be cleaned based on the operator.
                // State cleanup applies to the side with LARGER values.
                // For `left < right` or `left <= right`: RIGHT is larger → clean RIGHT state
                // For `left > right` or `left >= right`: LEFT is larger → clean LEFT state
                let left_is_larger =
                    matches!(op, ExprType::GreaterThan | ExprType::GreaterThanOrEqual);

                let (clean_left, clean_right) = if left_is_larger {
                    // Left side is larger, we can clean left state
                    let do_clean =
                        !equal_condition_clean_state && clean_left_state_conjunction_idx.is_none();
                    if do_clean {
                        clean_left_state_conjunction_idx = Some(conjunction_idx);
                    }
                    (do_clean, false)
                } else {
                    // Right side is larger, we can clean right state
                    let do_clean =
                        !equal_condition_clean_state && clean_right_state_conjunction_idx.is_none();
                    if do_clean {
                        clean_right_state_conjunction_idx = Some(conjunction_idx);
                    }
                    (false, do_clean)
                };

                let mut is_valuable_inequality = clean_left || clean_right;

                // Add watermark columns for the inequality.
                // We can only yield watermark from the LARGER side downstream.
                // For `left >= right`: left is larger, yield left watermark
                // For `left <= right`: right is larger, yield right watermark
                if left_is_larger {
                    if let Some(internal) = l2i.try_map(left_idx)
                        && !watermark_columns.contains(internal)
                    {
                        watermark_columns.insert(internal, ctx.next_watermark_group_id());
                        is_valuable_inequality = true;
                    }
                } else if let Some(internal) = r2i.try_map(right_idx)
                    && !watermark_columns.contains(internal)
                {
                    watermark_columns.insert(internal, ctx.next_watermark_group_id());
                    is_valuable_inequality = true;
                }

                if is_valuable_inequality {
                    inequality_pairs.push((
                        clean_left,
                        clean_right,
                        InequalityInputPairV2::new(left_idx, right_idx, op),
                    ));
                }
            }
            watermark_columns.map_clone(&core.i2o_col_mapping())
        };

        // TODO: derive from input
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            stream_kind,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );

        Ok(Self {
            base,
            core,
            inequality_pairs,
            is_append_only: stream_kind.is_append_only(),
            clean_left_state_conjunction_idx,
            clean_right_state_conjunction_idx,
        })
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    /// Get a reference to the hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        self.core
            .on
            .as_eq_predicate_ref()
            .expect("StreamHashJoin should store predicate as EqJoinPredicate")
    }

    /// Convert this hash join to a delta join plan
    pub fn into_delta_join(self) -> StreamDeltaJoin {
        StreamDeltaJoin::new(self.core).unwrap()
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

    pub fn inequality_pairs(&self) -> &Vec<(bool, bool, InequalityInputPairV2)> {
        &self.inequality_pairs
    }
}

impl Distill for StreamHashJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let (ljk, rjk) = self
            .eq_join_predicate()
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

impl PlanTreeNodeBinary<Stream> for StreamHashJoin {
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
        Self::new(core).unwrap()
    }
}

impl_plan_tree_node_for_binary! { Stream, StreamHashJoin }

impl TryToStreamPb for StreamHashJoin {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        let left_jk_indices = self.eq_join_predicate().left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate().right_eq_indexes();
        let left_jk_indices_prost = left_jk_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_jk_indices_prost = right_jk_indices.iter().map(|idx| *idx as i32).collect_vec();

        let retract =
            self.left().stream_kind().is_retract() || self.right().stream_kind().is_retract();

        let dk_indices_in_jk = self.derive_dist_key_in_join_key();

        let (left_table, left_degree_table, left_deduped_input_pk_indices) =
            Join::infer_internal_and_degree_table_catalog(
                self.left(),
                left_jk_indices,
                dk_indices_in_jk.clone(),
            );
        let (right_table, right_degree_table, right_deduped_input_pk_indices) =
            Join::infer_internal_and_degree_table_catalog(
                self.right(),
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

        let null_safe_prost = self.eq_join_predicate().null_safes().into_iter().collect();

        let condition = self
            .eq_join_predicate()
            .other_cond()
            .as_expr_unless_true()
            .map(|expr| expr.to_expr_proto_checked_pure(retract, "JOIN condition"))
            .transpose()?;

        // Helper function to convert ExprType to PbInequalityType
        fn expr_type_to_pb_inequality_type(op: ExprType) -> i32 {
            match op {
                ExprType::LessThan => PbInequalityType::LessThan as i32,
                ExprType::LessThanOrEqual => PbInequalityType::LessThanOrEqual as i32,
                ExprType::GreaterThan => PbInequalityType::GreaterThan as i32,
                ExprType::GreaterThanOrEqual => PbInequalityType::GreaterThanOrEqual as i32,
                _ => PbInequalityType::Unspecified as i32,
            }
        }

        Ok(NodeBody::HashJoin(Box::new(HashJoinNode {
            join_type: self.core.join_type as i32,
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            condition,
            // Deprecated: keep empty for new plans
            inequality_pairs: vec![],
            // New inequality pairs with clearer semantics
            inequality_pairs_v2: self
                .inequality_pairs
                .iter()
                .map(|(clean_left, clean_right, pair)| PbInequalityPairV2 {
                    left_idx: pair.left_idx as u32,
                    right_idx: pair.right_idx as u32,
                    clean_left_state: *clean_left,
                    clean_right_state: *clean_right,
                    op: expr_type_to_pb_inequality_type(pair.op),
                })
                .collect_vec(),
            left_table: Some(left_table.to_internal_table_prost()),
            right_table: Some(right_table.to_internal_table_prost()),
            left_degree_table: Some(left_degree_table.to_internal_table_prost()),
            right_degree_table: Some(right_degree_table.to_internal_table_prost()),
            left_deduped_input_pk_indices,
            right_deduped_input_pk_indices,
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            is_append_only: self.is_append_only,
            // Join encoding type should now be read from per-job config override.
            #[allow(deprecated)]
            join_encoding_type: PbJoinEncodingType::Unspecified as _,
        })))
    }
}

impl ExprRewritable<Stream> for StreamHashJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).unwrap().into()
    }
}

impl ExprVisitable for StreamHashJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
