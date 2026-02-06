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

//! DataFusion Aggregate Function Integration for RisingWave
//!
//! This module bridges RisingWave's aggregate functions to DataFusion's execution engine,
//! enabling RisingWave aggregates to be executed within DataFusion's query planning and
//! execution framework.
//!
//! # Architecture Overview
//!
//! DataFusion expects all aggregate functions to follow a two-phase aggregation model:
//! 1. **Partial Phase**: Compute partial aggregates on input data
//! 2. **Final Phase**: Merge partial aggregates to produce final results
//!
//! However, RisingWave has a different model where only certain aggregate functions
//! (like `sum`, `count`, `avg`) support two-phase aggregation. These functions are
//! able to use `partial_to_total` to define distinct implementations for each phase.
//! Other aggregates (like `median`, `string_agg`, etc.) only have a single implementation
//! that handles the entire aggregation process.
//!
//! # Three Implementation Strategies
//!
//! To bridge the semantic gap between RisingWave and DataFusion, we provide three
//! implementation strategies, selected based on aggregate type and data type compatibility:
//!
//! ## 1. DataFusion Native (Fastest Path)
//!
//! Uses DataFusion's built-in aggregate implementations directly, providing the best performance.
//!
//! **Key characteristics:**
//! - Zero conversion overhead - uses DataFusion's native Arrow-based execution
//! - Fully optimized for DataFusion's execution engine
//! - Selected when aggregate type AND all data types are DataFusion-native
//!
//! **Eligibility check** ([`is_datafusion_native_agg`]):
//! - Aggregate type is in [`is_datafusion_native_agg_type`] list
//! - All input types are DataFusion-native (no `Serial`, `Jsonb`, `Struct`, etc.)
//! - Return type is DataFusion-native
//! - All ORDER BY column types are DataFusion-native
//! - No direct arguments (e.g., percentile values)
//!
//! **Supported aggregates**: `sum`, `sum0`, `min`, `max`, `count`, `avg`, `var_pop`, `var_samp`,
//! `stddev_pop`, `stddev_samp`, `bit_and`, `bit_or`, `bit_xor`, `bool_and`, `bool_or`,
//! `first_value`, `last_value`
//!
//! **Conversion function**: [`convert_datafusion_native_agg_func`]
//!
//! ## 2. Two-Phase Wrapper ([`two_phase::AggregateFunction`])
//!
//! Used when DataFusion native is not available, but RisingWave has separate partial/final
//! implementations (i.e., `agg_type.partial_to_total()` returns `Some`).
//!
//! **Key characteristics:**
//! - Uses distinct RisingWave functions for partial (`phase1`) and final (`phase2`) aggregation
//! - Leverages RisingWave's native intermediate state representation
//! - During `update_batch`: Uses `phase1` function to compute partial state
//! - During `state`: Serializes `phase1` result (e.g., partial sum)
//! - During `merge_batch`: Feeds serialized state to `phase2` function
//!
//! **Example aggregates**: `sum` on non-native types, `count` with special handling
//! - `sum`: Phase 1 computes partial sums, Phase 2 sums the partial sums
//! - `max`: Phase 1 computes partial max, Phase 2 finds max of partial maxes
//!
//! **State representation:**
//! ```text
//! Phase 1 (Partial):  Input → phase1_agg → Intermediate State
//! Phase 2 (Final):    Intermediate State → phase2_agg → Final Result
//! ```
//!
//! ## 3. Single-Phase Wrapper ([`single_phase::AggregateFunction`])
//!
//! Used for aggregates that do **not** have separate partial/final implementations in RisingWave
//! (i.e., `agg_type.partial_to_total()` returns `None`).
//!
//! **Key characteristics:**
//! - Uses the same RisingWave aggregate function for both DataFusion's partial and final phases
//! - Serializes intermediate state as binary-encoded arrays of input values
//! - During `update_batch`: Accumulates input rows into array builders
//! - During `state`: Serializes accumulated arrays into binary format
//! - During `merge_batch`: Deserializes binary state back to arrays and processes them
//!
//! **Example aggregates**: `median`, `mode`, `percentile_cont`, `string_agg`
//!
//! **State representation:**
//! ```text
//! Phase 1 (Partial):  [ArrayBuilder] → serialize → [Binary]
//! Phase 2 (Final):    [Binary] → deserialize → [Array] → aggregate → Result
//! ```
//!
//! # Decision Logic
//!
//! The [`convert_agg_call`] function determines which implementation to use:
//!
//! ```text
//! if is_datafusion_native_agg(agg):
//!     Use DataFusion native UDF (fastest path)
//!         - convert_datafusion_native_agg_func()
//! else if agg.agg_type.partial_to_total() is Some:
//!     Use two_phase::AggregateFunction
//!         - Build phase1 aggregate (e.g., sum_partial)
//!         - Build phase2 aggregate (e.g., sum_total)
//! else:
//!     Use single_phase::AggregateFunction
//!         - Build single aggregate function
//!         - Handle state serialization/deserialization internally
//! ```
//!
//! # Example Flow for aggregation wrappers
//!
//! For `SUM(column::decimal)` using two-phase wrapper:
//! ```text
//! Input Data: [10, 20, 30, 40]
//!
//! Partition 1:                    Partition 2:
//! update_batch([10, 20])         update_batch([30, 40])
//! phase1: partial_sum = 30       phase1: partial_sum = 70
//!         ↓                              ↓
//!      state()                        state()
//!         ↓                              ↓
//!    ScalarValue(30)                ScalarValue(70)
//!         └──────────┬──────────────────┘
//!                    ↓
//!              merge_batch()
//!                    ↓
//!          phase2: sum([30, 70])
//!                    ↓
//!               evaluate()
//!                    ↓
//!              Result: 100
//! ```
//!
//! For `MEDIAN(column)` using single-phase wrapper:
//! ```text
//! Input Data: [10, 20, 30, 40]
//!
//! Partition 1:                    Partition 2:
//! update_batch([10, 20])         update_batch([30, 40])
//! arrays: [[10, 20]]             arrays: [[30, 40]]
//!         ↓                              ↓
//!      state()                        state()
//!         ↓                              ↓
//!   Binary(serialized)             Binary(serialized)
//!         └──────────┬──────────────────┘
//!                    ↓
//!              merge_batch()
//!                    ↓
//!     deserialize → [10, 20, 30, 40]
//!                    ↓
//!            update_phase2([10, 20, 30, 40])
//!                    ↓
//!               evaluate()
//!                    ↓
//!              Result: 25.0
//! ```
//!
//! # Aggregate Modifiers: DISTINCT, FILTER, and ORDER BY
//!
//! SQL aggregates can have modifiers like DISTINCT, FILTER, and ORDER BY. These are
//! handled differently based on DataFusion's capabilities:
//!
//! ## DISTINCT and FILTER (DataFusion Native)
//!
//! DataFusion can handle these modifiers natively in its aggregate execution framework:
//! - **FILTER**: DataFusion applies the filter predicate before passing rows to the accumulator
//! - **DISTINCT**: DataFusion deduplicates input values before aggregation
//!
//! These are passed through `AggregateFunctionParams` and processed by DataFusion's executor.
//!
//! ## ORDER BY (Requires `ProjectionOrderBy`)
//!
//! ORDER BY requires special handling because DataFusion requires all aggregate functions to manege
//! ordering semantics internally.
//! The `append_order_projection` helper function wraps aggregates with [`ProjectionOrderBy`] which:
//! - Appends ORDER BY columns to the aggregate input
//! - Sorts input values according to the ORDER BY specification
//! - Passes sorted values to the underlying aggregate function

use std::sync::Arc;

use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::bit_and_or_xor::{bit_and_udaf, bit_or_udaf, bit_xor_udaf};
use datafusion::functions_aggregate::bool_and_or::{bool_and_udaf, bool_or_udaf};
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use datafusion::logical_expr::expr::{
    AggregateFunction as DFAggregateFunction, AggregateFunctionParams as DFAggregateFunctionParams,
};
use datafusion::logical_expr::{AggregateUDF, Signature, TypeSignature, Volatility};
use datafusion::prelude::{Expr as DFExpr, lit};
use risingwave_common::bail_not_implemented;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::aggregate::{AggArgs, AggType, BoxedAggregateFunction, PbAggKind};
use risingwave_expr::expr::LiteralExpression;

use crate::datafusion::aggregate::orderby::ProjectionOrderBy;
use crate::datafusion::{
    CastExecutor, ColumnTrait, RwDataTypeDataFusionExt, convert_column_order, convert_expr,
};
use crate::error::Result as RwResult;
use crate::expr::Expr;
use crate::optimizer::plan_node::PlanAggCall;

mod orderby;
mod single_phase;
mod two_phase;

#[cfg(test)]
mod tests;

pub fn convert_agg_call(
    agg: &PlanAggCall,
    input_columns: &impl ColumnTrait,
) -> RwResult<DFAggregateFunction> {
    let native_agg = is_datafusion_native_agg(agg, input_columns);
    let func: Arc<AggregateUDF> = if native_agg {
        convert_datafusion_native_agg_func(&agg.agg_type)?
    } else {
        convert_agg_func_fallback(agg, input_columns)?
    };

    let mut df_args: Vec<_> = agg
        .inputs
        .iter()
        .map(|input_ref| DFExpr::Column(input_columns.column(input_ref.index)))
        .collect();
    if native_agg
        && df_args.is_empty()
        && matches!(agg.agg_type, AggType::Builtin(PbAggKind::Count))
    {
        // DataFusion will take COUNT(*) as COUNT(1)
        df_args.push(lit(1));
    }
    let filter = match agg.filter.as_expr_unless_true() {
        None => None,
        Some(expr) => Some(Box::new(convert_expr(&expr, input_columns)?)),
    };
    let order_by = agg
        .order_by
        .iter()
        .map(|order| convert_column_order(order, input_columns))
        .collect::<Vec<_>>();
    Ok(DFAggregateFunction {
        func,
        params: DFAggregateFunctionParams {
            args: df_args,
            distinct: agg.distinct,
            filter,
            order_by,
            null_treatment: None, // RisingWave doesn't support null treatment in agg calls yet
        },
    })
}

fn is_datafusion_native_agg(agg: &PlanAggCall, input_columns: &impl ColumnTrait) -> bool {
    is_datafusion_native_agg_type(&agg.agg_type)
        && agg
            .inputs
            .iter()
            .all(|input| input.data_type.is_datafusion_native())
        && agg.return_type.is_datafusion_native()
        && agg.order_by.iter().all(|order| {
            input_columns
                .rw_data_type(order.column_index)
                .is_datafusion_native()
        })
        && agg.direct_args.is_empty()
}

fn is_datafusion_native_agg_type(agg_type: &AggType) -> bool {
    if let AggType::Builtin(kind) = agg_type {
        return matches!(
            kind,
            PbAggKind::Sum
                | PbAggKind::Min
                | PbAggKind::Max
                | PbAggKind::Count
                | PbAggKind::Avg
                | PbAggKind::Sum0
                | PbAggKind::VarPop
                | PbAggKind::VarSamp
                | PbAggKind::StddevPop
                | PbAggKind::StddevSamp
                | PbAggKind::BitAnd
                | PbAggKind::BitOr
                | PbAggKind::BitXor
                | PbAggKind::BoolAnd
                | PbAggKind::BoolOr
                | PbAggKind::FirstValue
                | PbAggKind::LastValue
                | PbAggKind::ArrayAgg
        );
    }
    false
}

/// Convert an `AggType` to a DataFusion `AggregateUDF`.
///
/// This function is used for window functions that use aggregate functions.
pub fn convert_agg_type_to_udaf(agg_type: &AggType) -> RwResult<Arc<AggregateUDF>> {
    convert_datafusion_native_agg_func(agg_type)
}

fn convert_datafusion_native_agg_func(agg_type: &AggType) -> RwResult<Arc<AggregateUDF>> {
    let AggType::Builtin(kind) = agg_type else {
        bail_not_implemented!("only builtin agg types are supported");
    };
    match kind {
        PbAggKind::Sum | PbAggKind::Sum0 => Ok(sum_udaf()),
        PbAggKind::Min => Ok(min_udaf()),
        PbAggKind::Max => Ok(max_udaf()),
        PbAggKind::Count => Ok(count_udaf()),
        PbAggKind::Avg => Ok(avg_udaf()),
        PbAggKind::VarPop => Ok(var_pop_udaf()),
        PbAggKind::VarSamp => Ok(var_samp_udaf()),
        PbAggKind::StddevPop => Ok(stddev_pop_udaf()),
        PbAggKind::StddevSamp => Ok(stddev_udaf()),
        PbAggKind::BitAnd => Ok(bit_and_udaf()),
        PbAggKind::BitOr => Ok(bit_or_udaf()),
        PbAggKind::BitXor => Ok(bit_xor_udaf()),
        PbAggKind::BoolAnd => Ok(bool_and_udaf()),
        PbAggKind::BoolOr => Ok(bool_or_udaf()),
        PbAggKind::FirstValue => Ok(first_value_udaf()),
        PbAggKind::LastValue => Ok(last_value_udaf()),
        PbAggKind::ArrayAgg => Ok(array_agg_udaf()),
        _ => bail_not_implemented!(
            "Agg {:?} is not supported to convert to datafusion native impl yet",
            kind
        ),
    }
}

fn convert_agg_func_fallback(
    agg: &PlanAggCall,
    input_columns: &impl ColumnTrait,
) -> RwResult<Arc<AggregateUDF>> {
    let args: AggArgs = agg
        .inputs
        .iter()
        .map(|input_ref| (input_ref.data_type.clone(), input_ref.index))
        .collect();
    let direct_args = agg
        .direct_args
        .iter()
        .map(|arg| LiteralExpression::new(arg.return_type(), arg.get_data().clone()))
        .collect::<Vec<_>>();
    let signature = Signature {
        type_signature: TypeSignature::Any(agg.inputs.len()),
        volatility: Volatility::Volatile,
    };
    let cast = build_cast_executor(agg, input_columns)?;

    let func: AggregateUDF = if let Some(phase2_agg_type) = agg.agg_type.partial_to_total() {
        let mut phase1 =
            risingwave_expr::aggregate::build_append_only(&risingwave_expr::aggregate::AggCall {
                agg_type: agg.agg_type.clone(),
                args,
                return_type: agg.return_type.clone(),
                column_orders: vec![],
                filter: None,
                distinct: false,
                direct_args: direct_args.clone(),
            })?;
        phase1 = append_order_projection(phase1, agg)?;

        let phase2 =
            risingwave_expr::aggregate::build_append_only(&risingwave_expr::aggregate::AggCall {
                agg_type: phase2_agg_type,
                args: AggArgs::from_iter(std::iter::once((phase1.return_type(), 0))),
                return_type: agg.return_type.clone(),
                column_orders: vec![],
                filter: None,
                distinct: false,
                direct_args,
            })?;
        AggregateUDF::new_from_impl(two_phase::AggregateFunction::new(
            agg.agg_type.to_string(),
            cast,
            phase1,
            phase2,
            signature,
        )?)
    } else {
        let mut func =
            risingwave_expr::aggregate::build_append_only(&risingwave_expr::aggregate::AggCall {
                agg_type: agg.agg_type.clone(),
                args,
                return_type: agg.return_type.clone(),
                column_orders: vec![],
                filter: None,
                distinct: false,
                direct_args,
            })?;
        func = append_order_projection(func, agg)?;

        let input_types = agg
            .inputs
            .iter()
            .map(|input_ref| input_ref.data_type.clone())
            .chain(
                agg.order_by
                    .iter()
                    .map(|order| input_columns.rw_data_type(order.column_index)),
            )
            .collect::<Vec<_>>();
        AggregateUDF::new_from_impl(single_phase::AggregateFunction::new(
            agg.agg_type.to_string(),
            input_types,
            cast,
            func,
            signature,
        ))
    };

    tracing::debug!(
        "Agg {:?} will use risingwave fallback impl, it may be slower than native",
        agg
    );
    Ok(Arc::new(func))
}

fn build_cast_executor(
    agg: &PlanAggCall,
    input_columns: &impl ColumnTrait,
) -> RwResult<CastExecutor> {
    let source = agg
        .inputs
        .iter()
        .map(|input_ref| input_columns.df_data_type(input_ref.index))
        .chain(
            agg.order_by
                .iter()
                .map(|order| input_columns.df_data_type(order.column_index)),
        );
    let target = agg
        .inputs
        .iter()
        .map(|input_ref| input_ref.data_type.clone())
        .chain(
            agg.order_by
                .iter()
                .map(|order| input_columns.rw_data_type(order.column_index)),
        );
    let cast = CastExecutor::from_iter(source, target)?;
    Ok(cast)
}

fn append_order_projection(
    func: BoxedAggregateFunction,
    agg: &PlanAggCall,
) -> RwResult<BoxedAggregateFunction> {
    if agg.order_by.is_empty() {
        return Ok(func);
    }

    let arg_types = agg
        .inputs
        .iter()
        .map(|input_ref| input_ref.data_type.clone())
        .collect();
    let arg_indices: Vec<usize> = (0..agg.inputs.len()).collect();
    let column_orders = agg
        .order_by
        .iter()
        .enumerate()
        .map(|(i, order)| ColumnOrder::new(i + arg_indices.len(), order.order_type))
        .collect();
    Ok(Box::new(ProjectionOrderBy::new(
        arg_types,
        arg_indices,
        column_orders,
        func,
    )))
}
