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
use risingwave_common::bail;
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::functional::SameOrElseExt;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    HashJoinNode, HashJoinWatermarkHandleDesc, InequalityPairV2 as PbInequalityPairV2,
    InequalityType as PbInequalityType, JoinKeyWatermarkIndex, PbJoinEncodingType,
};

use super::generic::GenericPlanNode;
use super::stream::prelude::*;
use super::stream_join_common::StreamJoinCommon;
use super::utils::{
    Distill, TableCatalogBuilder, childless_record, plan_node_name, watermark_pretty,
};
use super::{
    ExprRewritable, PlanBase, PlanTreeNodeBinary, StreamDeltaJoin, StreamPlanRef as PlanRef,
    TryToStreamPb, generic,
};
use crate::TableCatalog;
use crate::expr::{Expr, ExprDisplay, ExprRewriter, ExprType, ExprVisitor, InequalityInputPair};
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

    /// Join-key positions and cleaning flags for watermark handling.
    watermark_indices_in_jk: Vec<(usize, bool)>,

    /// `(conjunction_idx, clean_left_state, clean_right_state, InequalityInputPair)`.
    /// Each entry represents an inequality condition like `left_col <op> right_col`.
    /// The `conjunction_idx` is the index in `eq_join_predicate.other_cond().conjunctions`.
    inequality_pairs: Vec<(usize, bool, bool, InequalityInputPair)>,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    is_append_only: bool,
}

/// Result of watermark derivation for hash join.
struct WatermarkDeriveResult {
    /// Output watermark columns.
    watermark_columns: WatermarkColumns,
    /// Join-key positions and cleaning flags for watermark handling.
    watermark_indices_in_jk: Vec<(usize, bool)>,
    /// `(conjunction_idx, clean_left_state, clean_right_state, InequalityInputPair)`.
    inequality_pairs: Vec<(usize, bool, bool, InequalityInputPair)>,
}

/// Derives watermark columns and state cleaning info for hash join.
///
/// This function analyzes equal and inequality conditions to determine:
/// 1. Which join keys have watermarks on both sides (enabling state cleanup via equal conditions)
/// 2. Which inequality conditions can be used for state cleanup
/// 3. Which columns can emit watermarks downstream
///
/// **Important**: Only ONE column per table is allowed to do state cleaning.
/// Priority: join key (leftmost in PK) > inequality pairs.
fn derive_watermark_for_hash_join(
    core: &generic::Join<PlanRef>,
    eq_join_predicate: &EqJoinPredicate,
) -> WatermarkDeriveResult {
    let ctx = core.ctx();
    let l2i = core.l2i_col_mapping();
    let r2i = core.r2i_col_mapping();

    let mut watermark_indices_in_jk = vec![];
    let mut inequality_pairs = vec![];

    // Track if we've already found a column for state cleaning.
    // Join key state cleaning cleans both sides, so we track it separately.
    let mut found_jk_state_clean = false;
    let mut found_ineq_clean_left = false;
    let mut found_ineq_clean_right = false;

    // Process equal conditions: check if both sides have watermarks on join keys.
    // Only the FIRST (leftmost) join key with watermarks on both sides can do state cleaning.
    // This prioritizes the leftmost join key in PK order (since we reorder join keys
    // to put watermark columns first).
    let mut watermark_columns = WatermarkColumns::new();
    for (idx, (left_key, right_key)) in eq_join_predicate.eq_indexes().iter().enumerate() {
        if let Some(l_wtmk_group) = core.left.watermark_columns().get_group(*left_key)
            && let Some(r_wtmk_group) = core.right.watermark_columns().get_group(*right_key)
        {
            // Only the first join key with watermarks on both sides can do state cleaning
            let do_state_cleaning = !found_jk_state_clean;
            if do_state_cleaning {
                found_jk_state_clean = true;
            }
            watermark_indices_in_jk.push((idx, do_state_cleaning));

            if let Some(internal) = l2i.try_map(*left_key) {
                watermark_columns.insert(
                    internal,
                    l_wtmk_group.same_or_else(r_wtmk_group, || ctx.next_watermark_group_id()),
                );
            }
            if let Some(internal) = r2i.try_map(*right_key) {
                watermark_columns.insert(
                    internal,
                    l_wtmk_group.same_or_else(r_wtmk_group, || ctx.next_watermark_group_id()),
                );
            }
        }
    }

    // Process inequality pairs using the V2 format.
    // Inequality pairs can only clean state if no join key is doing state cleaning.
    let original_inequality_pairs = eq_join_predicate.inequality_pairs_v2();
    for (conjunction_idx, pair) in original_inequality_pairs {
        let InequalityInputPair {
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
        let left_is_larger = matches!(op, ExprType::GreaterThan | ExprType::GreaterThanOrEqual);

        // Inequality pairs can only clean state if:
        // 1. No join key is doing state cleaning (join key has priority)
        // 2. No other inequality pair has claimed this side yet
        let (clean_left, clean_right) = if left_is_larger {
            let do_clean = !found_jk_state_clean && !found_ineq_clean_left;
            if do_clean {
                found_ineq_clean_left = true;
            }
            (do_clean, false)
        } else {
            let do_clean = !found_jk_state_clean && !found_ineq_clean_right;
            if do_clean {
                found_ineq_clean_right = true;
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
                conjunction_idx,
                clean_left,
                clean_right,
                InequalityInputPair::new(left_idx, right_idx, op),
            ));
        }
    }

    let watermark_columns = watermark_columns.map_clone(&core.i2o_col_mapping());

    WatermarkDeriveResult {
        watermark_columns,
        watermark_indices_in_jk,
        inequality_pairs,
    }
}

impl StreamHashJoin {
    pub fn new(mut core: generic::Join<PlanRef>) -> Result<Self> {
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

        // Derive watermark columns and state cleaning info
        let WatermarkDeriveResult {
            watermark_columns,
            watermark_indices_in_jk,
            inequality_pairs,
        } = derive_watermark_for_hash_join(&core, &eq_join_predicate);

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
            watermark_indices_in_jk,
            inequality_pairs,
            is_append_only: stream_kind.is_append_only(),
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

    pub fn inequality_pairs(&self) -> &Vec<(usize, bool, bool, InequalityInputPair)> {
        &self.inequality_pairs
    }

    /// Returns the conjunction index of the first inequality that cleans left state, if any.
    fn clean_left_state_conjunction_idx(&self) -> Option<usize> {
        self.inequality_pairs
            .iter()
            .find(|(_, clean_left, _, _)| *clean_left)
            .map(|(idx, _, _, _)| *idx)
    }

    /// Returns the conjunction index of the first inequality that cleans right state, if any.
    fn clean_right_state_conjunction_idx(&self) -> Option<usize> {
        self.inequality_pairs
            .iter()
            .find(|(_, _, clean_right, _)| *clean_right)
            .map(|(idx, _, _, _)| *idx)
    }

    /// Infer internal table catalog and degree table catalog for hash join.
    ///
    /// This method also infers `clean_watermark_indices` based on:
    /// 1. Equal conditions: if a join key has watermarks on both sides, the corresponding
    ///    column can be used for state cleaning.
    /// 2. Inequality conditions: if `clean_left_state` or `clean_right_state` is true,
    ///    the corresponding column can be used for state cleaning.
    ///
    /// # Arguments
    /// * `input` - The input plan (left or right side of the join)
    /// * `join_key_indices` - The indices of join keys in the input schema
    /// * `dk_indices_in_jk` - The indices of distribution keys in join keys
    /// * `is_left` - Whether this is for the left side of the join
    ///
    /// # Returns
    /// A tuple of (`internal_table`, `degree_table`, `deduped_input_pk_indices`)
    fn infer_internal_and_degree_table_catalog(
        &self,
        input: PlanRef,
        join_key_indices: Vec<usize>,
        dk_indices_in_jk: Vec<usize>,
        is_left: bool,
    ) -> Result<(TableCatalog, TableCatalog, Vec<usize>)> {
        let schema = input.schema();

        let internal_table_dist_keys = dk_indices_in_jk
            .iter()
            .map(|idx| join_key_indices[*idx])
            .collect_vec();

        let degree_table_dist_keys = dk_indices_in_jk.clone();

        // The pk of hash join internal and degree table should be join_key + input_pk.
        let join_key_len = join_key_indices.len();
        let mut pk_indices = join_key_indices.clone();

        // dedup the pk in dist key..
        let mut deduped_input_pk_indices = vec![];
        for input_pk_idx in input.stream_key().unwrap() {
            if !pk_indices.contains(input_pk_idx)
                && !deduped_input_pk_indices.contains(input_pk_idx)
            {
                deduped_input_pk_indices.push(*input_pk_idx);
            }
        }

        pk_indices.extend(deduped_input_pk_indices.clone());

        // Infer clean_watermark_indices for internal tablestate cleaning
        let (
            clean_watermark_indices,
            eq_join_key_clean_watermark_indices,
            inequal_clean_watermark_indices,
        ) = self.infer_clean_watermark_indices(&join_key_indices, is_left)?;

        // Build internal table
        let mut internal_table_catalog_builder = TableCatalogBuilder::default();
        let internal_columns_fields = schema.fields().to_vec();

        internal_columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
        });

        internal_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk.clone());
        internal_table_catalog_builder.set_clean_watermark_indices(clean_watermark_indices);

        // Build degree table.
        let mut degree_table_catalog_builder = TableCatalogBuilder::default();

        let degree_column_field = Field::with_name(DataType::Int64, "_degree");

        pk_indices.iter().enumerate().for_each(|(order_idx, idx)| {
            degree_table_catalog_builder.add_column(&internal_columns_fields[*idx]);
            degree_table_catalog_builder.add_order_column(order_idx, OrderType::ascending());
        });

        // Add degree column
        degree_table_catalog_builder.add_column(&degree_column_field);
        let degree_col_idx = degree_table_catalog_builder.columns().len() - 1;

        // Add inequality column after _degree if this side has inequality-based cleaning
        let degree_inequality_col_idx = inequal_clean_watermark_indices
            .iter()
            .at_most_one()
            .unwrap()
            .map(|idx| {
                degree_table_catalog_builder.add_column(&internal_columns_fields[*idx]);
                degree_table_catalog_builder.columns().len() - 1
            });

        // Set value indices: always include _degree, optionally include inequality column
        let mut value_indices = vec![degree_col_idx];
        if let Some(idx) = degree_inequality_col_idx {
            value_indices.push(idx);
        }
        degree_table_catalog_builder.set_value_indices(value_indices);

        degree_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk);

        // Set clean watermark indices: use inequality column if present, otherwise use join key.
        let degree_clean_watermark_indices = if let Some(idx) = degree_inequality_col_idx {
            vec![idx]
        } else {
            // Note: eq_join_key_clean_watermark_indices are input-schema indices, but degree table
            // columns are remapped via pk_indices (join_key first, then deduped pk).
            eq_join_key_clean_watermark_indices
                .iter()
                .map(|input_col_idx| {
                    join_key_indices
                        .iter()
                        .position(|jk_idx| jk_idx == input_col_idx)
                        .expect("eq join key clean watermark index must exist in join_key_indices")
                })
                .collect()
        };
        degree_table_catalog_builder.set_clean_watermark_indices(degree_clean_watermark_indices);

        Ok((
            internal_table_catalog_builder.build(internal_table_dist_keys, join_key_len),
            degree_table_catalog_builder.build(degree_table_dist_keys, join_key_len),
            deduped_input_pk_indices,
        ))
    }

    /// Infer which columns can be used for state cleaning based on watermark information.
    ///
    /// For the left/right internal table:
    /// 1. From equal conditions: if `watermark_indices_in_jk[i].1` is true (`do_state_cleaning`),
    ///    then the join key at position i can be used for cleaning both sides.
    /// 2. From inequality conditions:
    ///    - If `clean_left_state` is true, the left column (`left_idx`) can be used for cleaning left state.
    ///    - If `clean_right_state` is true, the right column (`right_idx`) can be used for cleaning right state.
    fn infer_clean_watermark_indices(
        &self,
        join_key_indices: &[usize],
        is_left: bool,
    ) -> Result<(Vec<usize>, Vec<usize>, Vec<usize>)> {
        let mut clean_watermark_indices = vec![];

        // From equal conditions: if do_state_cleaning is true, the join key column can clean state
        let eq_join_key_clean_watermark_indices =
            self.infer_eq_join_key_clean_watermark_indices(join_key_indices);
        clean_watermark_indices.extend(eq_join_key_clean_watermark_indices.clone());
        let mut inequal_clean_watermark_indices = vec![];
        // From inequality conditions: check clean_left_state or clean_right_state
        for (_conjunction_idx, clean_left, clean_right, pair) in &self.inequality_pairs {
            if is_left && *clean_left {
                let col_idx = pair.left_idx;
                if !clean_watermark_indices.contains(&col_idx) {
                    inequal_clean_watermark_indices.push(col_idx);
                }
            } else if !is_left && *clean_right {
                let col_idx = pair.right_idx;
                if !clean_watermark_indices.contains(&col_idx) {
                    inequal_clean_watermark_indices.push(col_idx);
                }
            }
        }

        clean_watermark_indices.extend(inequal_clean_watermark_indices.clone());

        // Verify: only 1 column per table is allowed to do state cleaning.
        // This invariant is enforced by `derive_watermark_for_hash_join`.
        if clean_watermark_indices.len() > 1 {
            bail!(
                "Expected at most 1 clean_watermark_index per table, got {:?}",
                clean_watermark_indices
            )
        }

        Ok((
            clean_watermark_indices,
            eq_join_key_clean_watermark_indices,
            inequal_clean_watermark_indices,
        ))
    }

    /// Infer which join keys can be used for state cleaning based on equal conditions.
    fn infer_eq_join_key_clean_watermark_indices(&self, join_key_indices: &[usize]) -> Vec<usize> {
        let mut clean_indices = vec![];
        for (idx_in_jk, do_state_cleaning) in &self.watermark_indices_in_jk {
            if *do_state_cleaning {
                let col_idx = join_key_indices[*idx_in_jk];
                if !clean_indices.contains(&col_idx) {
                    clean_indices.push(col_idx);
                }
            }
        }
        clean_indices
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

        let clean_left_state_conjunction_idx = self.clean_left_state_conjunction_idx();
        let clean_right_state_conjunction_idx = self.clean_right_state_conjunction_idx();
        let clean_state_in_jk_indices: Vec<usize> = self
            .watermark_indices_in_jk
            .iter()
            .filter_map(
                |(idx, do_state_cleaning)| if *do_state_cleaning { Some(*idx) } else { None },
            )
            .collect();

        let name = plan_node_name!("StreamHashJoin",
            { "window", self.left().watermark_columns().contains(ljk) && self.right().watermark_columns().contains(rjk) },
            { "interval", clean_left_state_conjunction_idx.is_some() && clean_right_state_conjunction_idx.is_some() },
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

        let get_other_cond = |conjunction_idx| {
            Pretty::debug(&ExprDisplay {
                expr: &self.eq_join_predicate().other_cond().conjunctions[conjunction_idx],
                input_schema: &concat_schema,
            })
        };
        let get_eq_cond = |conjunction_idx| {
            Pretty::debug(&ExprDisplay {
                expr: &self.eq_join_predicate().eq_cond().conjunctions[conjunction_idx],
                input_schema: &concat_schema,
            })
        };
        if !clean_state_in_jk_indices.is_empty() {
            vec.push((
                "conditions_to_clean_state_in_join_key",
                Pretty::Array(
                    clean_state_in_jk_indices
                        .iter()
                        .map(|idx| get_eq_cond(*idx))
                        .collect::<Vec<_>>(),
                ),
            ));
        }
        if let Some(i) = clean_left_state_conjunction_idx {
            vec.push(("conditions_to_clean_left_state_table", get_other_cond(i)));
        }
        if let Some(i) = clean_right_state_conjunction_idx {
            vec.push(("conditions_to_clean_right_state_table", get_other_cond(i)));
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

        let (left_table, left_degree_table, left_deduped_input_pk_indices) = self
            .infer_internal_and_degree_table_catalog(
                self.left(),
                left_jk_indices,
                dk_indices_in_jk.clone(),
                true, // is_left
            )?;
        let (right_table, right_degree_table, right_deduped_input_pk_indices) = self
            .infer_internal_and_degree_table_catalog(
                self.right(),
                right_jk_indices,
                dk_indices_in_jk,
                false, // is_left
            )?;

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
            watermark_handle_desc: Some(HashJoinWatermarkHandleDesc {
                watermark_indices_in_jk: self
                    .watermark_indices_in_jk
                    .iter()
                    .map(|(idx, do_clean)| JoinKeyWatermarkIndex {
                        index: *idx as u32,
                        do_state_cleaning: *do_clean,
                    })
                    .collect(),
                inequality_pairs: self
                    .inequality_pairs
                    .iter()
                    .map(
                        |(_conjunction_idx, clean_left, clean_right, pair)| PbInequalityPairV2 {
                            left_idx: pair.left_idx as u32,
                            right_idx: pair.right_idx as u32,
                            clean_left_state: *clean_left,
                            clean_right_state: *clean_right,
                            op: expr_type_to_pb_inequality_type(pair.op),
                        },
                    )
                    .collect(),
            }),
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
