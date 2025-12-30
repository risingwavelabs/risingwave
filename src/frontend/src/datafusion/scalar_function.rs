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

//! Integration of RisingWave scalar functions with DataFusion.
//!
//! This module provides the bridge between RisingWave's scalar function system and DataFusion's
//! expression evaluation engine. It enables RisingWave scalar functions to be executed within
//! DataFusion query plans.
//!
//! ## Overview
//!
//! The main entry point is [`convert_function_call`], which converts a RisingWave [`FunctionCall`]
//! into a DataFusion expression ([`DFExpr`]). The module uses rule-like abstractions to determine
//! whether a RisingWave expression can be directly converted to a native DataFusion expression,
//! or if it requires wrapping with [`ScalarUDFImpl`].
//!
//! If an expression matches one of the conversion rules (unary operations, binary operations,
//! case expressions, or cast expressions), it is converted directly to the corresponding DataFusion
//! expression. Otherwise, the expression falls back to being wrapped in [`RwScalarFunction`],
//! which uses RisingWave's native scalar function execution engine within DataFusion's query plan.
//!
//! ## RisingWave Scalar Function Wrapper
//!
//! For functions that DataFusion cannot handle directly, [`RwScalarFunction`] implements
//! [`ScalarUDFImpl`] to wrap RisingWave's expression evaluation logic. This allows seamless
//! execution of RisingWave functions within DataFusion's query execution engine.
//!
//! The wrapper handles:
//! - Type casting from DataFusion types to RisingWave types
//! - Data chunk creation and manipulation
//! - Async expression evaluation using RisingWave's executor
//! - Result conversion back to DataFusion-compatible types

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as DFDataType;
use datafusion::functions::{math, unicode};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    BinaryExpr, Case, Cast, ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use datafusion::prelude::Expr as DFExpr;
use datafusion_common::Result as DFResult;
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType as RwDataType;
use risingwave_expr::expr::{BoxedExpression, ValueImpl, build_from_prost};

use crate::datafusion::{
    CastExecutor, ColumnTrait, convert_expr, convert_scalar_value, create_data_chunk,
    to_datafusion_error,
};
use crate::error::{Result as RwResult, RwError};
use crate::expr::{Expr, ExprType, FunctionCall};

pub fn convert_function_call(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> RwResult<DFExpr> {
    macro_rules! try_rules {
        ( $($func:ident),* ) => {
            $(
                if let Some(df_expr) = $func(func_call, input_columns) {
                    return Ok(df_expr);
                }
            )*
        };
    }
    try_rules!(
        convert_unary_func,
        convert_binary_func,
        convert_case_func,
        convert_cast_func,
        convert_trivial_datafusion_func,
        fallback_rw_expr_builder
    );

    bail_not_implemented!(
        "DataFusionPlanConverter: unsupported function call expression {:?}",
        func_call
    );
}

fn convert_unary_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.inputs().len() != 1 {
        return None;
    }

    macro_rules! match_unary_expr {
        ($arg:expr, $(($func_type:ident, $df_expr_variant:ident)),*) => {
            match $arg {
                $(
                ExprType::$func_type => {
                    let arg = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
                    Some(DFExpr::$df_expr_variant(Box::new(arg)))
                }
                )*
                _ => None
            }
        };
    }
    match_unary_expr!(
        func_call.func_type(),
        (Not, Not),
        (IsNotNull, IsNotNull),
        (IsNull, IsNull),
        (IsTrue, IsTrue),
        (IsFalse, IsFalse),
        (Neg, Negative)
    )
}

fn convert_binary_op(expr_type: ExprType) -> Option<Operator> {
    macro_rules! match_binary_op {
        ( $(($rw_type:ident, $df_type:ident)),* ) => {
            match expr_type {
                $(
                    ExprType::$rw_type => Some(Operator::$df_type),
                )*
                _ => None
            }
        };
    }
    match_binary_op!(
        (Equal, Eq),
        (NotEqual, NotEq),
        (LessThan, Lt),
        (LessThanOrEqual, LtEq),
        (GreaterThan, Gt),
        (GreaterThanOrEqual, GtEq),
        (Add, Plus),
        (Subtract, Minus),
        (Multiply, Multiply),
        (Divide, Divide),
        (Modulus, Modulo),
        (And, And),
        (Or, Or),
        (IsDistinctFrom, IsDistinctFrom),
        // datafusion's regex misses some features, waiting https://github.com/apache/datafusion/issues/18778
        // (RegexpMatch, RegexMatch),
        (Like, LikeMatch),
        (ILike, ILikeMatch),
        (BitwiseAnd, BitwiseAnd),
        (BitwiseOr, BitwiseOr),
        (BitwiseXor, BitwiseXor),
        (BitwiseShiftRight, BitwiseShiftRight),
        (BitwiseShiftLeft, BitwiseShiftLeft)
    )
}

fn convert_binary_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.inputs().len() != 2 {
        return None;
    }
    if !func_call
        .inputs()
        .iter()
        .all(|input| input.return_type().is_datafusion_native())
    {
        return None;
    }

    let op = convert_binary_op(func_call.func_type())?;
    let left = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
    let right = convert_expr(&func_call.inputs()[1], input_columns).ok()?;
    Some(DFExpr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn convert_case_func(func_call: &FunctionCall, input_columns: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.func_type() != ExprType::Case {
        return None;
    }

    let mut args = func_call
        .inputs()
        .iter()
        .map(|arg| {
            let expr = convert_expr(arg, input_columns).ok()?;
            Some(Box::new(expr))
        })
        .collect::<Option<Vec<_>>>()?;

    let else_expr = if args.len() % 2 == 1 {
        Some(args.pop().unwrap())
    } else {
        None
    };
    let when_then_expr = args.into_iter().tuples().collect();
    Some(DFExpr::Case(Case {
        expr: None,
        when_then_expr,
        else_expr,
    }))
}

fn convert_cast_func(func_call: &FunctionCall, input_columns: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.inputs().len() != 1 {
        return None;
    }
    if func_call.func_type() != ExprType::Cast {
        return None;
    }

    let rw_from_type = func_call.inputs()[0].return_type();
    let rw_to_type = func_call.return_type();
    if !can_cast_by_datafusion(&rw_from_type, &rw_to_type) {
        return None;
    }

    let arg = convert_expr(&func_call.inputs()[0], input_columns).ok()?;
    let target_type = IcebergArrowConvert
        .to_arrow_field("", &func_call.return_type())
        .ok()?
        .data_type()
        .clone();
    Some(DFExpr::Cast(Cast {
        expr: Box::new(arg),
        data_type: target_type,
    }))
}

// TODO: verify more cast safety cases
// case 1: both from and to are datafusion native types
fn can_cast_by_datafusion(from: &RwDataType, to: &RwDataType) -> bool {
    from.is_datafusion_native() && to.is_datafusion_native()
}

#[easy_ext::ext(RwDataTypeDataFusionExt)]
impl RwDataType {
    pub fn is_datafusion_native(&self) -> bool {
        match self {
            RwDataType::Boolean
            | RwDataType::Int32
            | RwDataType::Int64
            | RwDataType::Float32
            | RwDataType::Float64
            | RwDataType::Date
            | RwDataType::Time
            | RwDataType::Timestamp
            | RwDataType::Timestamptz
            | RwDataType::Varchar
            | RwDataType::Bytea
            | RwDataType::Serial
            | RwDataType::Decimal => true,
            RwDataType::Struct(v) => v.types().all(RwDataTypeDataFusionExt::is_datafusion_native),
            RwDataType::List(list) => list.elem().is_datafusion_native(),
            RwDataType::Map(map) => {
                map.key().is_datafusion_native() && map.value().is_datafusion_native()
            }
            _ => false,
        }
    }

    pub fn to_datafusion_native(&self) -> Option<DFDataType> {
        if !self.is_datafusion_native() {
            return None;
        }
        let arrow_field = IcebergArrowConvert.to_arrow_field("", self).ok()?;
        Some(arrow_field.data_type().clone())
    }
}

fn convert_trivial_datafusion_func(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    if !func_call
        .inputs()
        .iter()
        .all(|input| input.return_type().is_datafusion_native())
    {
        return None;
    }
    if !func_call.return_type().is_datafusion_native() {
        return None;
    }

    let udf_impl: Arc<ScalarUDF> = match func_call.func_type() {
        ExprType::Round => math::round(),
        ExprType::Abs => math::abs(),
        ExprType::Ceil => math::ceil(),
        ExprType::Floor => math::floor(),
        ExprType::Trunc => math::trunc(),
        ExprType::Pow => math::power(),
        ExprType::Exp => math::exp(),
        ExprType::Sin => math::sin(),
        ExprType::Cos => math::cos(),
        ExprType::Tan => math::tan(),
        ExprType::Cot => math::cot(),
        ExprType::Asin => math::asin(),
        ExprType::Acos => math::acos(),
        ExprType::Atan => math::atan(),
        ExprType::Atan2 => math::atan2(),
        ExprType::Sqrt => math::sqrt(),
        ExprType::Degrees => math::degrees(),
        ExprType::Radians => math::radians(),
        ExprType::Sinh => math::sinh(),
        ExprType::Cosh => math::cosh(),
        ExprType::Tanh => math::tanh(),
        ExprType::Asinh => math::asinh(),
        ExprType::Acosh => math::acosh(),
        ExprType::Atanh => math::atanh(),
        ExprType::Ln => math::ln(),
        ExprType::Log10 => math::log10(),
        ExprType::Substr => unicode::substr(),
        _ => return None,
    };
    let args = func_call
        .inputs()
        .iter()
        .map(|input| convert_expr(input, input_columns).ok())
        .collect::<Option<Vec<_>>>()?;

    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: udf_impl,
        args,
    }))
}

fn fallback_rw_expr_builder(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    tracing::warn!(
        "Falling back to RwScalarFunction for function call, it may have performance impact: {:?}",
        func_call
    );

    let cast = CastExecutor::from_iter(
        (0..input_columns.len()).map(|i| input_columns.df_data_type(i)),
        (0..input_columns.len()).map(|i| input_columns.rw_data_type(i)),
    )
    .ok()?;
    let boxed_expr = build_from_prost(&func_call.try_to_expr_proto().ok()?).ok()?;

    let column_name = (0..input_columns.len())
        .map(|i| input_columns.column(i).flat_name())
        .collect::<Vec<_>>();

    let volatility = match func_call.is_pure() {
        true => Volatility::Immutable,
        false => Volatility::Volatile,
    };
    let func = RwScalarFunction {
        name: format!("RisingWave Scalar Function ({:?})", func_call),
        column_name,
        cast,
        expr: boxed_expr,
        signature: Signature {
            type_signature: TypeSignature::Any(input_columns.len()),
            volatility,
        },
    };
    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: Arc::new(ScalarUDF::new_from_impl(func)),
        args: (0..input_columns.len())
            .map(|i| DFExpr::Column(input_columns.column(i)))
            .collect(),
    }))
}

#[derive(Debug, educe::Educe)]
#[educe(PartialEq, Eq, Hash)]
struct RwScalarFunction {
    // Datafusion uses function name as column identifier, so we need to keep unique names for different functions to avoid conflicts
    name: String,
    column_name: Vec<String>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    cast: CastExecutor,
    #[educe(PartialEq(ignore), Hash(ignore))]
    expr: BoxedExpression,
    #[educe(PartialEq(ignore), Hash(ignore))]
    signature: Signature,
}

impl ScalarUDFImpl for RwScalarFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DFDataType]) -> DFResult<DFDataType> {
        let field = IcebergArrowConvert
            .to_arrow_field("", &self.expr.return_type())
            .map_err(to_datafusion_error)?;
        Ok(field.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = args
            .args
            .into_iter()
            .map(|cv| cv.into_array(1))
            .collect::<DFResult<Vec<_>>>()?;
        let chunk =
            create_data_chunk(arrays.into_iter(), args.number_rows).map_err(to_datafusion_error)?;

        let value = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let chunk = self.cast.execute(chunk).await?;
                let value = self.expr.eval_v2(&chunk).await?;
                Ok::<ValueImpl, RwError>(value)
            })
        })
        .map_err(to_datafusion_error)?;

        let res = match value {
            ValueImpl::Array(array_impl) => {
                let array = IcebergArrowConvert
                    .to_arrow_array(args.return_field.data_type(), &array_impl)
                    .map_err(to_datafusion_error)?;
                ColumnarValue::Array(array)
            }
            ValueImpl::Scalar { value, .. } => {
                let value = convert_scalar_value(&value, self.expr.return_type())
                    .map_err(to_datafusion_error)?;
                ColumnarValue::Scalar(value)
            }
        };
        Ok(res)
    }
}
