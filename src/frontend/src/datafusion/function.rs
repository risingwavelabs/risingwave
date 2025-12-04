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

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType as DFDataType, Field, Fields, Schema};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    BinaryExpr, Case, Cast, ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use datafusion::prelude::Expr as DFExpr;
use datafusion_common::{Column, Result as DFResult, ScalarValue};
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::error::BoxedError;
use risingwave_common::types::DataType as RwDataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{BoxedExpression, ValueImpl, build_from_prost};

use crate::datafusion::{ColumnTrait, convert_expr, convert_scalar_value};
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprType, FunctionCall};

pub fn convert_function_call(
    func_call: &FunctionCall,
    input_schema: &impl ColumnTrait,
) -> RwResult<DFExpr> {
    macro_rules! try_rules {
        ( $($func:ident),* ) => {
            $(
                if let Some(df_expr) = $func(func_call, input_schema) {
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
        fallback_rw_expr_builder
    );

    bail_not_implemented!(
        "DataFusionPlanConverter: unsupported function call expression {:?}",
        func_call
    );
}

fn convert_unary_func(func_call: &FunctionCall, input_schema: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.inputs().len() != 1 {
        return None;
    }

    macro_rules! match_unary_expr {
        ($arg:expr, $(($func_type:ident, $df_expr_variant:ident)),*) => {
            match $arg {
                $(
                ExprType::$func_type => {
                    let arg = convert_expr(&func_call.inputs()[0], input_schema).ok()?;
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
        (RegexpMatch, RegexMatch),
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
    input_schema: &impl ColumnTrait,
) -> Option<DFExpr> {
    if func_call.inputs().len() != 2 {
        return None;
    }
    let op = convert_binary_op(func_call.func_type())?;
    let left = convert_expr(&func_call.inputs()[0], input_schema).ok()?;
    let right = convert_expr(&func_call.inputs()[1], input_schema).ok()?;
    Some(DFExpr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn convert_case_func(func_call: &FunctionCall, input_schema: &impl ColumnTrait) -> Option<DFExpr> {
    if func_call.func_type() != ExprType::Case {
        return None;
    }

    let mut args = func_call
        .inputs()
        .iter()
        .map(|arg| {
            let expr = convert_expr(arg, input_schema).ok()?;
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

fn convert_cast_func(func_call: &FunctionCall, input_schema: &impl ColumnTrait) -> Option<DFExpr> {
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

    let arg = convert_expr(&func_call.inputs()[0], input_schema).ok()?;
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
// case 1: IcebergArrowConvert just converts Jsonb to varchar. So any cast from or to Jsonb should not be handled by DataFusion
fn can_cast_by_datafusion(from: &RwDataType, to: &RwDataType) -> bool {
    fn contain_jsonb(dt: &RwDataType) -> bool {
        match dt {
            RwDataType::Jsonb => true,
            RwDataType::Struct(v) => v.types().any(contain_jsonb),
            RwDataType::List(list) => contain_jsonb(list.elem()),
            RwDataType::Map(map) => contain_jsonb(map.key()) || contain_jsonb(map.value()),
            _ => false,
        }
    }
    !contain_jsonb(from) && !contain_jsonb(to)
}

// TODO: handle children type casting. For example, (DATE - INTERVAL), datafusion will interpret INTERVAL as string, we need to cast it to Interval type before calling risingwave boxed expression.
fn fallback_rw_expr_builder(
    func_call: &FunctionCall,
    input_columns: &impl ColumnTrait,
) -> Option<DFExpr> {
    let boxed_expr = build_from_prost(&func_call.try_to_expr_proto().ok()?).ok()?;

    let column_vec = (0..input_columns.len())
        .map(|i| input_columns.column(i))
        .collect::<Vec<_>>();

    let func = RwScalarFunction {
        name: format!(
            "RisingWave Scalar Function (type: {})",
            func_call.func_type().as_str_name()
        ),
        expr: boxed_expr,
        input_columns: column_vec,
        signature: Signature {
            type_signature: TypeSignature::Any(input_columns.len()),
            volatility: Volatility::Volatile,
        },
    };
    Some(DFExpr::ScalarFunction(ScalarFunction {
        func: Arc::new(ScalarUDF::new_from_impl(func)),
        // TODO(opt): analyze dependent columns to reduce the costs of arguments
        args: (0..input_columns.len())
            .map(|i| DFExpr::Column(input_columns.column(i)))
            .collect(),
    }))
}

#[derive(Debug)]
struct RwScalarFunction {
    name: String,
    expr: BoxedExpression,
    input_columns: Vec<Column>,
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
            .map_err(boxed)?;
        Ok(field.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        let opts = RecordBatchOptions::new()
            .with_match_field_names(false)
            .with_row_count(Some(args.number_rows));
        let columns = args
            .args
            .into_iter()
            .map(|cv| cv.into_array(1))
            .collect::<DFResult<Vec<_>>>()?;
        let input_fields: Fields = columns
            .iter()
            .zip_eq_fast(self.input_columns.iter())
            .map(|(array, col)| Field::new(col.name(), array.data_type().clone(), true))
            .collect();
        let input_schema = Arc::new(Schema::new(input_fields));
        let record_batch = RecordBatch::try_new_with_options(input_schema, columns, &opts)?;
        let chunk = IcebergArrowConvert
            .chunk_from_record_batch(&record_batch)
            .map_err(boxed)?;
        // TODO: better way to run async eval in sync context
        let value = futures::executor::block_on(self.expr.eval_v2(&chunk)).map_err(boxed)?;
        let res = match value {
            ValueImpl::Array(array_impl) => {
                let array = IcebergArrowConvert
                    .to_arrow_array(args.return_type, &array_impl)
                    .map_err(boxed)?;
                ColumnarValue::Array(array)
            }
            ValueImpl::Scalar { value, .. } => {
                let value = match value {
                    Some(v) => convert_scalar_value(&v, self.expr.return_type()).map_err(boxed)?,
                    None => ScalarValue::Null,
                };
                ColumnarValue::Scalar(value)
            }
        };
        Ok(res)
    }
}

fn boxed<E: std::error::Error + Send + Sync + 'static>(e: E) -> BoxedError {
    // Convert to anyhow::Error first to capture backtrace information.
    anyhow::Error::new(e).into()
}
