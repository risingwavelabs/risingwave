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
use crate::expr::{Expr, ExprType, FunctionCall};
use crate::optimizer::optimizer_context::MaterializedViewCandidate;
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalMultiJoinBuilder, LogicalPlanRef, LogicalProject,
    LogicalScan, PlanTreeNodeUnary,
};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::{ColIndexMapping, Condition, IndexSet};

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
        Self::inner_join_rewrite(plan, candidate)
            .or_else(|| Self::agg_rollup_rewrite(plan, candidate))
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

    fn inner_join_rewrite(
        plan: &PlanRef,
        candidate: &MaterializedViewCandidate,
    ) -> Option<PlanRef> {
        let query_join = Self::extract_inner_join_rewrite(plan)?;
        let mv_join = Self::extract_inner_join_rewrite(&candidate.plan)?;
        if query_join.inputs.len() != mv_join.inputs.len() {
            return None;
        }

        let mv_input_to_query = Self::match_join_inputs(&query_join.inputs, &mv_join.inputs)?;
        let query_input_to_query = (0..query_join.inputs.len()).collect::<Vec<_>>();
        let query_base_offsets = Self::input_base_offsets(&query_join.inputs);
        let total_base_len = query_join.total_base_len();

        let query_conditions = Self::normalize_join_conditions(
            &query_join,
            &query_input_to_query,
            &query_base_offsets,
            total_base_len,
        )?;
        let mv_conditions = Self::normalize_join_conditions(
            &mv_join,
            &mv_input_to_query,
            &query_base_offsets,
            total_base_len,
        )?;
        let rewritten_predicate = Self::subtract_conditions(query_conditions, mv_conditions)?;

        let query_output_to_base =
            Self::normalize_join_outputs(&query_join, &query_input_to_query, &query_base_offsets)?;
        let mv_output_to_base =
            Self::normalize_join_outputs(&mv_join, &mv_input_to_query, &query_base_offsets)?;
        let Some(base_to_mv_output) =
            Self::invert_mapping(&mv_output_to_base, query_join.total_base_len())
        else {
            return None;
        };
        let rewritten_predicate =
            Self::rewrite_condition_to_mv(rewritten_predicate, &base_to_mv_output)?;
        let output_col_idx = query_output_to_base
            .iter()
            .map(|base_idx| base_to_mv_output.get(*base_idx).copied().flatten())
            .collect::<Option<Vec<_>>>()?;

        let mv_scan: LogicalPlanRef = Self::candidate_scan(candidate, plan.ctx())?.into();
        let rewritten = LogicalFilter::create(mv_scan, rewritten_predicate);
        if output_col_idx.iter().copied().eq(0..output_col_idx.len()) {
            Some(rewritten)
        } else {
            Some(LogicalProject::with_out_col_idx(rewritten, output_col_idx.into_iter()).into())
        }
    }

    fn extract_inner_join_rewrite(plan: &PlanRef) -> Option<InnerJoinRewrite> {
        let multijoin = LogicalMultiJoinBuilder::new(plan.clone());
        let (output_indices, conjunctions, inputs, _) = multijoin.into_parts();
        if inputs.len() < 2 {
            return None;
        }

        let inputs = inputs
            .iter()
            .map(Self::extract_join_leaf)
            .collect::<Option<Vec<_>>>()?;

        Some(InnerJoinRewrite {
            inputs,
            conditions: Condition { conjunctions },
            output_indices,
        })
    }

    fn extract_join_leaf(plan: &PlanRef) -> Option<JoinLeafRewrite> {
        if let Some(scan) = plan.as_logical_scan() {
            let output_to_base = scan.output_col_idx().clone();
            let base_scan =
                scan.clone_with_output_indices((0..scan.table().columns().len()).collect());
            let predicate = Self::rewrite_condition(
                scan.predicate().clone(),
                output_to_base.clone(),
                base_scan.schema().len(),
            )?;
            return Some(JoinLeafRewrite {
                base_scan: base_scan.into(),
                output_to_base,
                predicate,
            });
        }
        if let Some(filter) = plan.as_logical_filter() {
            let mut child = Self::extract_join_leaf(&filter.input())?;
            let predicate = Self::rewrite_condition(
                filter.predicate().clone(),
                child.output_to_base.clone(),
                child.base_len(),
            )?;
            child.predicate = child.predicate.and(predicate);
            return Some(child);
        }
        if let Some(project) = plan.as_logical_project() {
            let child = Self::extract_join_leaf(&project.input())?;
            let output_to_base = project
                .try_as_projection()?
                .into_iter()
                .map(|idx| child.output_to_base.get(idx).copied())
                .collect::<Option<Vec<_>>>()?;
            return Some(JoinLeafRewrite {
                base_scan: child.base_scan,
                output_to_base,
                predicate: child.predicate,
            });
        }
        None
    }

    fn input_base_offsets(inputs: &[JoinLeafRewrite]) -> Vec<usize> {
        let mut offsets = Vec::with_capacity(inputs.len());
        let mut offset = 0;
        for input in inputs {
            offsets.push(offset);
            offset += input.base_len();
        }
        offsets
    }

    fn match_join_inputs(
        query_inputs: &[JoinLeafRewrite],
        mv_inputs: &[JoinLeafRewrite],
    ) -> Option<Vec<usize>> {
        fn dfs(
            mv_idx: usize,
            query_inputs: &[JoinLeafRewrite],
            mv_inputs: &[JoinLeafRewrite],
            used_query: &mut [bool],
            mapping: &mut [usize],
        ) -> bool {
            if mv_idx == mv_inputs.len() {
                return true;
            }
            for query_idx in 0..query_inputs.len() {
                if used_query[query_idx]
                    || mv_inputs[mv_idx].base_scan != query_inputs[query_idx].base_scan
                    || mv_inputs[mv_idx].base_len() != query_inputs[query_idx].base_len()
                {
                    continue;
                }
                used_query[query_idx] = true;
                mapping[mv_idx] = query_idx;
                if dfs(mv_idx + 1, query_inputs, mv_inputs, used_query, mapping) {
                    return true;
                }
                used_query[query_idx] = false;
            }
            false
        }

        let mut used_query = vec![false; query_inputs.len()];
        let mut mapping = vec![usize::MAX; mv_inputs.len()];
        dfs(0, query_inputs, mv_inputs, &mut used_query, &mut mapping).then_some(mapping)
    }

    fn normalize_join_conditions(
        join: &InnerJoinRewrite,
        input_to_query: &[usize],
        query_base_offsets: &[usize],
        total_base_len: usize,
    ) -> Option<Condition> {
        let join_output_to_base =
            Self::join_output_to_base_mapping(join, input_to_query, query_base_offsets);
        let mut conditions =
            Self::rewrite_condition(join.conditions.clone(), join_output_to_base, total_base_len)?;
        for (input_idx, input) in join.inputs.iter().enumerate() {
            conditions = conditions.and(Self::shift_condition_to_join_base(
                input.predicate.clone(),
                *query_base_offsets.get(*input_to_query.get(input_idx)?)?,
                input.base_len(),
                total_base_len,
            )?);
        }
        Some(conditions)
    }

    fn normalize_join_outputs(
        join: &InnerJoinRewrite,
        input_to_query: &[usize],
        query_base_offsets: &[usize],
    ) -> Option<Vec<usize>> {
        let join_output_to_base =
            Self::join_output_to_base_mapping(join, input_to_query, query_base_offsets);
        join.output_indices
            .iter()
            .map(|idx| join_output_to_base.get(*idx).copied())
            .collect()
    }

    fn join_output_to_base_mapping(
        join: &InnerJoinRewrite,
        input_to_query: &[usize],
        query_base_offsets: &[usize],
    ) -> Vec<usize> {
        join.inputs
            .iter()
            .enumerate()
            .flat_map(|(input_idx, input)| {
                let offset = query_base_offsets[input_to_query[input_idx]];
                input
                    .output_to_base
                    .iter()
                    .copied()
                    .map(move |base_idx| base_idx + offset)
            })
            .collect()
    }

    fn subtract_conditions(query: Condition, mv: Condition) -> Option<Condition> {
        let mut counts: HashMap<crate::expr::ExprImpl, usize> = HashMap::new();
        for expr in query.conjunctions {
            *counts.entry(expr).or_default() += 1;
        }
        for expr in mv.conjunctions {
            let count = counts.get_mut(&expr)?;
            *count -= 1;
            if *count == 0 {
                counts.remove(&expr);
            }
        }
        let residual = counts
            .into_iter()
            .flat_map(|(expr, count)| std::iter::repeat_n(expr, count))
            .collect();
        Some(Self::canonicalize_condition(Condition {
            conjunctions: residual,
        }))
    }

    fn rewrite_condition(
        condition: Condition,
        output_to_base: Vec<usize>,
        target_size: usize,
    ) -> Option<Condition> {
        let mut mapping =
            ColIndexMapping::new(output_to_base.into_iter().map(Some).collect(), target_size);
        Some(Self::canonicalize_condition(
            condition.rewrite_expr(&mut mapping),
        ))
    }

    fn shift_condition_to_join_base(
        condition: Condition,
        offset: usize,
        source_size: usize,
        target_size: usize,
    ) -> Option<Condition> {
        let mut mapping = ColIndexMapping::new(
            (0..source_size).map(|idx| Some(idx + offset)).collect(),
            target_size,
        );
        Some(Self::canonicalize_condition(
            condition.rewrite_expr(&mut mapping),
        ))
    }

    fn invert_mapping(mapping: &[usize], source_size: usize) -> Option<Vec<Option<usize>>> {
        let mut inverse = vec![None; source_size];
        for (target_idx, source_idx) in mapping.iter().copied().enumerate() {
            match inverse.get_mut(source_idx)? {
                slot @ None => *slot = Some(target_idx),
                Some(_) => {}
            }
        }
        Some(inverse)
    }

    fn rewrite_condition_to_mv(
        condition: Condition,
        base_to_mv_output: &[Option<usize>],
    ) -> Option<Condition> {
        let input_refs = condition.collect_input_refs(base_to_mv_output.len());
        if input_refs
            .ones()
            .any(|idx| base_to_mv_output[idx].is_none())
        {
            return None;
        }
        let target_size = base_to_mv_output
            .iter()
            .flatten()
            .max()
            .copied()
            .map_or(0, |idx| idx + 1);
        let mut mapping = ColIndexMapping::new(base_to_mv_output.to_vec(), target_size);
        Some(Self::canonicalize_condition(
            condition.rewrite_expr(&mut mapping),
        ))
    }

    fn canonicalize_condition(condition: Condition) -> Condition {
        let mut conjunctions = condition
            .conjunctions
            .into_iter()
            .map(Self::canonicalize_expr)
            .collect::<Vec<_>>();
        conjunctions.sort_by_cached_key(|expr| format!("{expr:?}"));
        Condition { conjunctions }
    }

    fn canonicalize_expr(expr: crate::expr::ExprImpl) -> crate::expr::ExprImpl {
        if let Some((lhs, rhs)) = expr.as_eq_cond() {
            return FunctionCall::new_unchecked(
                ExprType::Equal,
                vec![lhs.into(), rhs.into()],
                expr.return_type(),
            )
            .into();
        }
        if let Some((lhs, rhs)) = expr.as_is_not_distinct_from_cond() {
            return FunctionCall::new_unchecked(
                ExprType::IsNotDistinctFrom,
                vec![lhs.into(), rhs.into()],
                expr.return_type(),
            )
            .into();
        }
        expr
    }
}

#[derive(Clone)]
struct JoinLeafRewrite {
    base_scan: LogicalPlanRef,
    output_to_base: Vec<usize>,
    predicate: Condition,
}

impl JoinLeafRewrite {
    fn base_len(&self) -> usize {
        self.base_scan.schema().len()
    }
}

#[derive(Clone)]
struct InnerJoinRewrite {
    inputs: Vec<JoinLeafRewrite>,
    conditions: Condition,
    output_indices: Vec<usize>,
}

impl InnerJoinRewrite {
    fn total_base_len(&self) -> usize {
        self.inputs.iter().map(JoinLeafRewrite::base_len).sum()
    }
}
