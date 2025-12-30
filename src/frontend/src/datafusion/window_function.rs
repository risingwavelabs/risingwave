use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::prelude::Expr as DFExpr;
use datafusion::sql::sqlparser::ast::NullTreatment;
use datafusion_common::ScalarValue;
use risingwave_common::{bail_not_implemented, not_implemented};
use risingwave_expr::window_function::{FrameBound, FrameBounds, RangeFrameOffset, WindowFuncKind};

use crate::datafusion::{ColumnTrait, convert_agg_type_to_udaf, convert_column_order};
use crate::error::Result as RwResult;
use crate::optimizer::plan_node::generic::PlanWindowFunction;

pub fn convert_window_expr(
    wf: &PlanWindowFunction,
    input_columns: &impl ColumnTrait,
) -> RwResult<DFExpr> {
    let fun = convert_window_func_kind(&wf.kind)?;

    let args: Vec<DFExpr> = wf
        .args
        .iter()
        .map(|input_ref| DFExpr::Column(input_columns.column(input_ref.index())))
        .collect();

    let partition_by: Vec<DFExpr> = wf
        .partition_by
        .iter()
        .map(|input_ref| DFExpr::Column(input_columns.column(input_ref.index())))
        .collect();

    let order_by: Vec<datafusion::logical_expr::SortExpr> = wf
        .order_by
        .iter()
        .map(|order| convert_column_order(order, input_columns))
        .collect();

    let window_frame = convert_frame(&wf.frame)?;

    let null_treatment = match wf.ignore_nulls {
        true => Some(NullTreatment::IgnoreNulls),
        false => Some(NullTreatment::RespectNulls),
    };

    Ok(DFExpr::WindowFunction(Box::new(WindowFunction {
        fun,
        params: WindowFunctionParams {
            args,
            partition_by,
            order_by,
            window_frame,
            filter: None, // RisingWave doesn't support FILTER in window functions at this level
            null_treatment,
            distinct: false, // RisingWave doesn't support DISTINCT in window functions
        },
    })))
}

fn convert_window_func_kind(kind: &WindowFuncKind) -> RwResult<WindowFunctionDefinition> {
    match kind {
        WindowFuncKind::RowNumber => Ok(WindowFunctionDefinition::WindowUDF(row_number_udwf())),
        WindowFuncKind::Rank => Ok(WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::rank::rank_udwf(),
        )),
        WindowFuncKind::DenseRank => Ok(WindowFunctionDefinition::WindowUDF(
            datafusion::functions_window::rank::dense_rank_udwf(),
        )),
        WindowFuncKind::Lag => Ok(WindowFunctionDefinition::WindowUDF(lag_udwf())),
        WindowFuncKind::Lead => Ok(WindowFunctionDefinition::WindowUDF(lead_udwf())),
        WindowFuncKind::Aggregate(agg_type) => Ok(WindowFunctionDefinition::AggregateUDF(
            convert_agg_type_to_udaf(agg_type).map_err(|_|
                not_implemented!("Window function with aggregate type {agg_type:?} not supported. It only supports builtin aggregate functions.")
            )?
        )),
    }
}

fn convert_frame(frame: &risingwave_expr::window_function::Frame) -> RwResult<WindowFrame> {
    match &frame.bounds {
        FrameBounds::Rows(rows_bounds) => {
            let start = convert_rows_frame_bound(&rows_bounds.start)?;
            let end = convert_rows_frame_bound(&rows_bounds.end)?;
            Ok(WindowFrame::new_bounds(WindowFrameUnits::Rows, start, end))
        }
        FrameBounds::Range(range_bounds) => {
            let start = convert_range_frame_bound(&range_bounds.start)?;
            let end = convert_range_frame_bound(&range_bounds.end)?;
            Ok(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end))
        }
        FrameBounds::Session(_) => {
            bail_not_implemented!("DataFusion does not support SESSION window frames")
        }
    }
}

fn convert_rows_frame_bound(bound: &FrameBound<usize>) -> RwResult<WindowFrameBound> {
    match bound {
        FrameBound::UnboundedPreceding => {
            Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None)))
        }
        FrameBound::Preceding(n) => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
            *n as u64,
        )))),
        FrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        FrameBound::Following(n) => Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
            *n as u64,
        )))),
        FrameBound::UnboundedFollowing => {
            Ok(WindowFrameBound::Following(ScalarValue::UInt64(None)))
        }
    }
}

fn convert_range_frame_bound(bound: &FrameBound<RangeFrameOffset>) -> RwResult<WindowFrameBound> {
    match bound {
        FrameBound::UnboundedPreceding => Ok(WindowFrameBound::Preceding(ScalarValue::Null)),
        FrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        FrameBound::UnboundedFollowing => Ok(WindowFrameBound::Following(ScalarValue::Null)),
        FrameBound::Preceding(_) | FrameBound::Following(_) => {
            bail_not_implemented!(
                "DataFusion conversion: RANGE frame with offset bounds not yet supported"
            )
        }
    }
}
