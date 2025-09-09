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

use std::collections::HashMap;

use risingwave_common::types::{DataType, StructType};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::{Agg, PlanAggCall};
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeUnary};

/// Unifies aggregation calls with the same pattern to reuse a single aggregation call with ROW construction.
///
/// This rule is particularly beneficial for streaming queries as it reduces the number of states maintained.
/// It supports `FIRST_VALUE` and `LAST_VALUE` aggregation functions that have the same ordering.
///
/// # Example transformation:
///
/// ## Before:
/// ```sql
/// SELECT
///   LAST_VALUE(col1 ORDER BY col2) AS last_col1,
///   LAST_VALUE(col3 ORDER BY col2) AS last_col3,
///   LAST_VALUE(col4 ORDER BY col2) AS last_col4,
///   LAST_VALUE(col5 ORDER BY col2) AS last_col5
/// FROM table_name
/// GROUP BY group_col;
/// ```
///
/// ## After:
/// ```sql
/// SELECT
///   (unified_last).f0 AS last_col1,
///   (unified_last).f1 AS last_col3,
///   (unified_last).f2 AS last_col4,
///   (unified_last).f3 AS last_col5
/// FROM (
///   SELECT
///     LAST_VALUE(ROW(col1, col3, col4, col5) ORDER BY col2) AS unified_last
///   FROM table_name GROUP BY group_col
/// ) sub;
/// ```
///
/// # Plan transformation:
///
/// ## Before:
/// ```text
/// LogicalAgg [group_col]
///  ├─agg_calls:
///  │  ├─ LAST_VALUE(col1 ORDER BY col2)     -- State 1
///  │  ├─ LAST_VALUE(col3 ORDER BY col2)     -- State 2
///  │  ├─ LAST_VALUE(col4 ORDER BY col2)     -- State 3
///  │  └─ LAST_VALUE(col5 ORDER BY col2)     -- State 4
///  └─LogicalScan { table: table_name }
/// ```
///
/// ## After:
/// ```text
/// LogicalProject
///  ├─exprs: [(unified_last).f0, (unified_last).f1, (unified_last).f2, (unified_last).f3]
///  └─LogicalAgg [group_col]
///     ├─agg_calls:
///     │  └─ LAST_VALUE(ROW(col1,col3,col4,col5) ORDER BY col2)  -- Single State!
///     └─LogicalProject
///        ├─exprs: [group_col, col1, col2, col3, col4, col5, ROW(col1,col3,col4,col5)]
///        └─LogicalScan { table: table_name }
/// ```
///
/// The key benefit: **4 aggregation states → 1 aggregation state** for streaming performance!
pub struct UnifySameAggCallPatternRule {}

impl Rule<Logical> for UnifySameAggCallPatternRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;

        let calls = agg.agg_calls();
        if calls.len() < 2 {
            // Need at least 2 calls to merge
            return None;
        }

        // Group calls by their "pattern" (agg_type + order_by + distinct + filter)
        let mut pattern_groups: HashMap<AggPattern, Vec<(usize, &PlanAggCall)>> = HashMap::new();

        for (idx, call) in calls.iter().enumerate() {
            if !self.is_supported_agg_type(&call.agg_type) {
                continue;
            }

            // Only support single input aggregations for now
            if call.inputs.len() != 1 {
                continue;
            }

            let pattern = AggPattern {
                agg_type: call.agg_type.clone(),
                order_by: call.order_by.clone(),
                distinct: call.distinct,
                filter: call.filter.clone(),
            };

            pattern_groups.entry(pattern).or_default().push((idx, call));
        }

        // Find ALL patterns with multiple calls that can be merged
        let mergeable_patterns: Vec<(AggPattern, Vec<(usize, &PlanAggCall)>)> = pattern_groups
            .into_iter()
            .filter(|(_, calls_in_pattern)| calls_in_pattern.len() >= 2)
            .collect();

        if mergeable_patterns.is_empty() {
            return None;
        }

        // Build the complete transformation pipeline:
        // 1. Pre-projection: Construct ROW expressions for each mergeable pattern
        // 2. Aggregation: Apply aggregation functions on ROW expressions
        // 3. Post-projection: Extract individual fields from ROW results

        // Step 1: Build pre-projection that constructs ROW expressions
        let input_plan = agg.input();
        let input_schema = input_plan.schema();
        let mut pre_proj_exprs = Vec::new();

        // Add all original input columns first
        for i in 0..input_schema.len() {
            pre_proj_exprs
                .push(InputRef::new(i, input_schema.fields()[i].data_type.clone()).into());
        }

        // Add ROW construction expressions for each mergeable pattern
        let mut pattern_to_row_col_idx = HashMap::new();
        for (pattern, mergeable_calls) in &mergeable_patterns {
            // Create ROW expression with inputs from this pattern
            let row_inputs: Vec<ExprImpl> = mergeable_calls
                .iter()
                .map(|(_, call)| call.inputs[0].clone().into())
                .collect();

            let named_fields = mergeable_calls
                .iter()
                .enumerate()
                .map(|(i, (_, call))| (format!("f{}", i), call.return_type.clone()))
                .collect::<Vec<_>>();
            let row_data_type = DataType::Struct(StructType::new(named_fields));

            let row_expr = FunctionCall::new_unchecked(ExprType::Row, row_inputs, row_data_type);

            let row_col_idx = pre_proj_exprs.len();
            pre_proj_exprs.push(row_expr.into());
            pattern_to_row_col_idx.insert(pattern.clone(), row_col_idx);
        }

        // Create pre-projection
        let pre_projection = LogicalProject::create(input_plan.clone(), pre_proj_exprs);

        // Step 2: Build new aggregation calls operating on ROW columns
        let mut new_calls = Vec::new();
        let mut original_to_output_mapping = Vec::new();

        // Process mergeable patterns
        for (pattern, mergeable_calls) in &mergeable_patterns {
            let row_col_idx = pattern_to_row_col_idx[pattern];
            let row_data_type = pre_projection.schema().fields()[row_col_idx]
                .data_type
                .clone();

            // Create aggregation call that operates on the ROW column
            let merged_call = PlanAggCall {
                agg_type: pattern.agg_type.clone(),
                return_type: row_data_type,
                inputs: vec![InputRef::new(
                    row_col_idx,
                    pre_projection.schema().fields()[row_col_idx]
                        .data_type
                        .clone(),
                )],
                distinct: pattern.distinct,
                order_by: pattern
                    .order_by
                    .iter()
                    .map(|order| {
                        // Adjust column references in ORDER BY to point to ROW column
                        ColumnOrder::new(row_col_idx, order.order_type)
                    })
                    .collect(),
                filter: pattern.filter.clone(),
                direct_args: vec![],
            };

            let merged_call_idx = new_calls.len();
            new_calls.push(merged_call);

            // Map each original call to its field in the merged result
            for (field_idx, (original_idx, _)) in mergeable_calls.iter().enumerate() {
                original_to_output_mapping.push((*original_idx, merged_call_idx, Some(field_idx)));
            }
        }

        // Add non-mergeable calls
        for (original_idx, call) in calls.iter().enumerate() {
            // Check if this call was already handled in mergeable patterns
            if !original_to_output_mapping
                .iter()
                .any(|(idx, _, _)| *idx == original_idx)
            {
                let new_call_idx = new_calls.len();
                new_calls.push(call.clone());
                original_to_output_mapping.push((original_idx, new_call_idx, None));
            }
        }

        // Sort mapping by original index to maintain order
        original_to_output_mapping.sort_by_key(|(original_idx, _, _)| *original_idx);

        if new_calls.len() >= calls.len() {
            // No benefit from optimization
            return None;
        }

        // Create aggregation on pre-projection
        let new_agg: PlanRef = Agg::new(new_calls, agg.group_key().clone(), pre_projection)
            .with_enable_two_phase(agg.core().two_phase_agg_enabled())
            .into();

        // Step 3: Build post-projection to extract fields from ROW results
        let mut post_proj_exprs = Vec::new();

        // Add group key columns
        for i in 0..agg.group_key().len() {
            let group_col_idx = agg.group_key().indices().nth(i).unwrap();
            post_proj_exprs.push(
                InputRef::new(i, input_schema.fields()[group_col_idx].data_type.clone()).into(),
            );
        }

        // Add aggregation result columns with proper field extraction
        for (original_idx, new_call_idx, field_idx_opt) in original_to_output_mapping {
            let original_return_type = calls[original_idx].return_type.clone();

            if let Some(field_idx) = field_idx_opt {
                // Extract field from ROW result using proper field access
                let struct_ref = InputRef::new(
                    agg.group_key().len() + new_call_idx,
                    new_agg.schema().fields()[agg.group_key().len() + new_call_idx]
                        .data_type
                        .clone(),
                );

                let field_access = FunctionCall::new_unchecked(
                    ExprType::Field,
                    vec![
                        struct_ref.into(),
                        Literal::new(Some((field_idx as i32).into()), DataType::Int32).into(),
                    ],
                    original_return_type,
                );

                post_proj_exprs.push(field_access.into());
            } else {
                // Direct reference to non-merged call
                post_proj_exprs.push(
                    InputRef::new(agg.group_key().len() + new_call_idx, original_return_type)
                        .into(),
                );
            }
        }

        Some(LogicalProject::create(new_agg, post_proj_exprs))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AggPattern {
    agg_type: AggType,
    order_by: Vec<ColumnOrder>,
    distinct: bool,
    filter: crate::utils::Condition,
}

impl UnifySameAggCallPatternRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }

    fn is_supported_agg_type(&self, agg_type: &AggType) -> bool {
        matches!(
            agg_type,
            AggType::Builtin(PbAggKind::FirstValue)
                | AggType::Builtin(PbAggKind::LastValue)
        )
    }
}
