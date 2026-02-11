// Copyright 2026 RisingWave Labs
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

use std::collections::HashMap;

use super::prelude::{PlanRef, *};
use crate::optimizer::optimizer_context::MaterializedViewCandidate;
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalPlanRef, LogicalProject, LogicalScan, PlanTreeNodeUnary,
};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::{ColIndexMapping, IndexSet};

pub struct MvSelectionRule;

impl MvSelectionRule {
    pub fn create() -> BoxedRule<Logical> {
        Box::new(Self)
    }
}

impl Rule<Logical> for MvSelectionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let ctx = plan.ctx();
        for candidate in ctx.batch_mview_candidates().iter() {
            if let Some(rewritten) = Self::rewrite_with_candidate(&plan, candidate) {
                return Some(rewritten);
            }
        }
        None
    }
}

impl MvSelectionRule {
    fn rewrite_with_candidate(
        plan: &PlanRef,
        candidate: &MaterializedViewCandidate,
    ) -> Option<PlanRef> {
        if plan == &candidate.plan {
            return Some(Self::candidate_scan(candidate, plan.ctx())?.into());
        }
        Self::agg_rollup_rewrite(plan, candidate)
    }

    fn candidate_scan(
        candidate: &MaterializedViewCandidate,
        ctx: crate::optimizer::optimizer_context::OptimizerContextRef,
    ) -> Option<LogicalScan> {
        let output_len = candidate.plan.schema().len();
        debug_assert!(output_len <= candidate.table.columns().len());
        let output_col_idx = (0..output_len).collect();
        Some(
            LogicalScan::create(candidate.table.clone(), ctx, None)
                .clone_with_output_indices(output_col_idx),
        )
    }

    /// Rewrite aggregate query by rolling up aggregate MV states.
    ///
    /// Graph (matching + rewrite):
    /// ```text
    /// 1) Normalize both sides to base-column lineage
    ///
    /// Query side                                  MV candidate side
    /// Agg_q(Gq, Aq)                               [Project_mv]?
    ///   └─ [Project_q]?                             └─ Agg_mv(Gm, Am)
    ///       └─ BaseInput                                  └─ [Project_mv_in]?
    ///                                                         └─ BaseInput
    ///
    /// 2) Match under lineage mapping
    /// - same BaseInput
    /// - map(Gq) ⊆ map(Gm)
    /// - for each aq in Aq, find semantically equivalent am in Am
    /// - aq must support partial_to_total
    ///
    /// 3) Build rewritten plan
    /// Agg_rollup(group = mapped Gq, calls = total(aq) on MV columns)
    ///   └─ Scan(MV table)
    /// [Project] (optional, restore query output order)
    /// ```
    fn agg_rollup_rewrite(
        plan: &PlanRef,
        candidate: &MaterializedViewCandidate,
    ) -> Option<PlanRef> {
        let query_agg = plan.as_logical_agg()?;
        let (mv_agg, mv_agg_to_mv_output) = Self::extract_mv_candidate_agg(candidate)?;

        if !query_agg.grouping_sets().is_empty() || !mv_agg.grouping_sets().is_empty() {
            return None;
        }
        // DISTINCT aggregates are not composable via partial-to-total rollup.
        if query_agg.agg_calls().iter().any(|call| call.distinct)
            || mv_agg.agg_calls().iter().any(|call| call.distinct)
        {
            return None;
        }

        let (query_base_input, query_input_to_base) =
            Self::agg_input_to_base_mapping(query_agg.clone())?;
        let (mv_base_input, mv_input_to_base) = Self::agg_input_to_base_mapping(mv_agg.clone())?;
        if query_base_input != mv_base_input {
            return None;
        }

        let mut mv_group_key_pos = HashMap::new();
        for (pos, col_idx) in mv_agg.group_key().indices().enumerate() {
            let mv_output_col_idx = *mv_agg_to_mv_output.get(pos)?;
            mv_group_key_pos.insert(*mv_input_to_base.get(col_idx)?, mv_output_col_idx);
        }

        let mut query_group_key_in_mv_output = Vec::with_capacity(query_agg.group_key().len());
        for col_idx in query_agg.group_key().indices() {
            let base_col_idx = *query_input_to_base.get(col_idx)?;
            query_group_key_in_mv_output.push(*mv_group_key_pos.get(&base_col_idx)?);
        }

        let mut normalized_call_to_mv_idx: HashMap<PlanAggCall, usize> = HashMap::new();
        let mut rewritten_agg_calls = Vec::with_capacity(query_agg.agg_calls().len());
        for query_call in query_agg.agg_calls() {
            let normalized_query_call = Self::normalize_agg_call(query_call, &query_input_to_base)?;
            let mv_call_idx = if let Some(mv_call_idx) = normalized_call_to_mv_idx
                .get(&normalized_query_call)
                .copied()
            {
                mv_call_idx
            } else {
                let mv_call_idx = mv_agg
                    .agg_calls()
                    .iter()
                    .enumerate()
                    .find(|(_, mv_call)| {
                        Self::normalize_agg_call(mv_call, &mv_input_to_base).is_some_and(
                            |normalized_mv_call| normalized_mv_call == normalized_query_call,
                        )
                    })?
                    .0;
                normalized_call_to_mv_idx.insert(normalized_query_call.clone(), mv_call_idx);
                mv_call_idx
            };

            // Only composable aggregate kinds can be rolled up from MV states.
            query_call.agg_type.partial_to_total()?;
            let mv_agg_output_col_idx = mv_agg.group_key().len() + mv_call_idx;
            let mv_output_col_idx = *mv_agg_to_mv_output.get(mv_agg_output_col_idx)?;
            rewritten_agg_calls.push(query_call.partial_to_total_agg_call(mv_output_col_idx));
        }

        let mv_scan: LogicalPlanRef = Self::candidate_scan(candidate, plan.ctx())?.into();

        let rewritten_group_key = IndexSet::from_iter(query_group_key_in_mv_output.iter().copied());
        let rewritten_agg: LogicalPlanRef =
            Agg::new(rewritten_agg_calls, rewritten_group_key.clone(), mv_scan)
                .with_enable_two_phase(query_agg.core().two_phase_agg_enabled())
                .into();

        let mut output_col_idx =
            Vec::with_capacity(query_agg.group_key().len() + query_agg.agg_calls().len());
        for group_col in query_group_key_in_mv_output {
            let col_pos = rewritten_group_key
                .indices()
                .position(|idx| idx == group_col)
                .expect("group key must exist");
            output_col_idx.push(col_pos);
        }
        output_col_idx
            .extend((0..query_agg.agg_calls().len()).map(|idx| rewritten_group_key.len() + idx));

        if output_col_idx.iter().copied().eq(0..output_col_idx.len()) {
            Some(rewritten_agg)
        } else {
            Some(LogicalProject::with_out_col_idx(rewritten_agg, output_col_idx.into_iter()).into())
        }
    }

    fn agg_input_to_base_mapping(agg: LogicalAgg) -> Option<(PlanRef, Vec<usize>)> {
        let agg_input = agg.input();
        if let Some(proj) = agg_input.as_logical_project() {
            let input_to_base = proj.try_as_projection()?;
            if let Some(scan) = proj.input().as_logical_scan() {
                let scan_base_columns = input_to_base
                    .iter()
                    .map(|idx| scan.output_col_idx().get(*idx).copied())
                    .collect::<Option<Vec<usize>>>()?;

                // Canonicalize scan shape to avoid false mismatch from different output column
                // subsets.
                let canonical_scan =
                    scan.clone_with_output_indices((0..scan.table().columns().len()).collect());
                Some((canonical_scan.into(), scan_base_columns))
            } else {
                Some((proj.input(), input_to_base))
            }
        } else if let Some(scan) = agg_input.as_logical_scan() {
            let scan_base_columns = scan.output_col_idx().clone();
            let canonical_scan =
                scan.clone_with_output_indices((0..scan.table().columns().len()).collect());
            Some((canonical_scan.into(), scan_base_columns))
        } else {
            None
        }
    }

    fn normalize_agg_call(call: &PlanAggCall, input_to_base: &[usize]) -> Option<PlanAggCall> {
        let mut normalized = call.clone();
        let index_mapping = ColIndexMapping::new(
            input_to_base
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<Option<usize>>>(),
            input_to_base.iter().max().copied().unwrap_or(0) + 1,
        );
        normalized.rewrite_input_index(index_mapping);
        Some(normalized)
    }

    fn extract_mv_candidate_agg(
        candidate: &MaterializedViewCandidate,
    ) -> Option<(LogicalAgg, Vec<usize>)> {
        if let Some(mv_agg) = candidate.plan.as_logical_agg() {
            return Some((mv_agg.clone(), (0..mv_agg.schema().len()).collect()));
        }

        let mv_project = candidate.plan.as_logical_project()?;
        let mv_project_input = mv_project.input();
        let mv_agg = mv_project_input.as_logical_agg()?;
        let mv_output_to_mv_agg = mv_project.try_as_projection()?;

        let mut mv_agg_to_mv_output = vec![None; mv_agg.schema().len()];
        for (mv_output_idx, mv_agg_idx) in mv_output_to_mv_agg.into_iter().enumerate() {
            if mv_agg_idx >= mv_agg_to_mv_output.len() {
                return None;
            }
            if mv_agg_to_mv_output[mv_agg_idx].is_none() {
                // Keep the first projected output for each agg column.
                // Duplicate projections (e.g. s1/s2 from the same agg output) are valid.
                mv_agg_to_mv_output[mv_agg_idx] = Some(mv_output_idx);
            }
        }
        let mv_agg_to_mv_output = mv_agg_to_mv_output
            .into_iter()
            .collect::<Option<Vec<usize>>>()?;
        Some((mv_agg.clone(), mv_agg_to_mv_output))
    }
}
