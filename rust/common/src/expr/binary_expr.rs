use crate::array::{Array, BoolArray, DataTypeTrait, I32Array, UTF8Array};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::expr_tmpl::BinaryExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::types::*;
use crate::vector_op::arithmetic_op::*;
use crate::vector_op::cmp::*;
use crate::vector_op::conjunction::{and, or};
use crate::vector_op::like::like_default;
use crate::vector_op::position::position;
use risingwave_proto::expr::ExprNode_Type;
use std::marker::PhantomData;

// this is a placeholder function that return bool in gen_binary_expr
pub fn cmp_placeholder<T1, T2, T3>(_l: T1, _r: T2) -> Result<bool> {
    Err(InternalError("The function is not supported".to_string()).into())
}
// this is a placeholder function that return T3 in gen_binary_expr
pub fn atm_placeholder<T1, T2, T3>(_l: T1, _r: T2) -> Result<T3> {
    Err(InternalError("The function is not supported".to_string()).into())
}

/// This macro helps create arithmetic expression.
/// It receive all the combinations of `gen_binary_expr` and generate corresponding match cases
/// In [], the parameters are for constructing new expression
/// * $l: left expression
/// * $r: right expression
/// * ret: return array type
/// In ()*, the parameters are for generating match cases
/// * $i1: left array type
/// * $i2: right array type
/// * $cast: The cast type in that the operation will calculate
/// * $func: The scalar function for expression, it's a generic function and specialized by the type of `$i1, $i2, $cast`
macro_rules! arithmetic_impl {
  ([$l:expr, $r:expr, $ret:expr], $( { $i1:ty, $i2:ty, $cast:ty, $func:ident} ),*) => {
    match ($l.return_type().data_type_kind(), $r.return_type().data_type_kind()) {
      $(
          (<$i1 as DataTypeTrait>::DATA_TYPE_ENUM, <$i2 as DataTypeTrait>::DATA_TYPE_ENUM) => {
            Box::new(BinaryExpression::< <$i1 as DataTypeTrait>::ArrayType, <$i2 as DataTypeTrait>::ArrayType, <$cast as DataTypeTrait>::ArrayType, _> {
              expr_ia1: $l,
              expr_ia2: $r,
              return_type: $ret,
              func: $func::< <<$i1 as DataTypeTrait>::ArrayType as Array>::OwnedItem, <<$i2 as DataTypeTrait>::ArrayType as Array>::OwnedItem, <<$cast as DataTypeTrait>::ArrayType as Array>::OwnedItem>,
              _phantom: PhantomData,
            })
          }
      ),*
      _ => {
        unimplemented!("The expression ({:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type().data_type_kind(), $r.return_type().data_type_kind())
      }
    }
  };
}

/// This macro helps create comparison expression. Its output array is a bool array
/// It receive all the combinations of `gen_binary_expr` and generate corresponding match cases
/// In [], the parameters are for constructing new expression
/// * $l: left expression
/// * $r: right expression
/// * ret: return array type
/// In ()*, the parameters are for generating match cases
/// * $i1: left array type
/// * $i2: right array type
/// * $cast: The cast type in that the operation will calculate
/// * $func: The scalar function for expression, it's a generic function and specialized by the type of `$i1, $i2, $cast`
macro_rules! comparison_impl {
  ([$l:expr, $r:expr, $ret:expr], $( { $i1:ty, $i2:ty, $cast:ty, $func: ident} ),*) => {
    match ($l.return_type().data_type_kind(), $r.return_type().data_type_kind()) {
      $(
          (<$i1 as DataTypeTrait>::DATA_TYPE_ENUM, <$i2 as DataTypeTrait>::DATA_TYPE_ENUM) => {
            Box::new(BinaryExpression::< <$i1 as DataTypeTrait>::ArrayType, <$i2 as DataTypeTrait>::ArrayType, BoolArray, _> {
              expr_ia1: $l,
              expr_ia2: $r,
              return_type: $ret,
              func: $func::< <<$i1 as DataTypeTrait>::ArrayType as Array>::OwnedItem, <<$i2 as DataTypeTrait>::ArrayType as Array>::OwnedItem, <<$cast as DataTypeTrait>::ArrayType as Array>::OwnedItem>,
              _phantom: PhantomData,
            })
          }
      ),*
      _ => {
        unimplemented!("The expression ({:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type().data_type_kind(), $r.return_type().data_type_kind())
      }
    }
  };
}

/// `gen_binary_expr` list all possible combination of input type and out put type
/// Specifically, the first type is left input, the second type is right input and the third is the cast type
/// For different data type, the scalar function may be different. Therefore we need to pass all possible scalar function
/// * `macro`: a macro helps create expression
/// * `int_f`: the scalar function of integer
/// * `float_f`: the scalar function of float
/// * `deci_f`: the scalar function for decimal with integer. In this scalar function, all inputs will be cast to decimal
/// * `deci_f_f`: the scalar function for decimal with float. In this scalar function, all inputs will be cast to float
macro_rules! gen_binary_expr {
  ($macro:tt, $int_f:ident, $float_f:ident, $deci_f_f:ident, $deci_f:ident, $interval_date_f:ident, $date_interval_f:ident $(, $x:tt)* ) => {
    $macro! {
      [$($x),*],
      { Int16Type, Int16Type, Int16Type, $int_f },
      { Int16Type, Int32Type, Int32Type, $int_f },
      { Int16Type, Int64Type, Int64Type, $int_f },
      { Int16Type, Float32Type, Float32Type, $float_f },
      { Int16Type, Float64Type, Float64Type, $float_f },
      { Int32Type, Int16Type, Int32Type, $int_f },
      { Int32Type, Int32Type, Int32Type, $int_f },
      { Int32Type, Int64Type, Int64Type, $int_f },
      { Int32Type, Float32Type, Float32Type, $float_f },
      { Int32Type, Float64Type, Float64Type, $float_f },
      { Int64Type, Int16Type,Int64Type, $int_f },
      { Int64Type, Int32Type,Int64Type, $int_f },
      { Int64Type, Int64Type, Int64Type, $int_f },
      { Int64Type, Float32Type, Float32Type , $float_f},
      { Int64Type, Float64Type, Float64Type, $float_f },
      { Float32Type, Int16Type, Float32Type, $float_f },
      { Float32Type, Int32Type, Float32Type, $float_f },
      { Float32Type, Int64Type, Float32Type , $float_f},
      { Float32Type, Float32Type, Float32Type, $float_f },
      { Float32Type, Float64Type, Float64Type, $float_f },
      { Float64Type, Int16Type, Float64Type, $float_f },
      { Float64Type, Int32Type, Float64Type, $float_f },
      { Float64Type, Int64Type, Float64Type, $float_f },
      { Float64Type, Float32Type, Float64Type, $float_f },
      { Float64Type, Float64Type, Float64Type, $float_f },
      { DecimalType, Int16Type, DecimalType, $deci_f },
      { DecimalType, Int32Type, DecimalType, $deci_f },
      { DecimalType, Int64Type, DecimalType, $deci_f },
      { DecimalType, Float32Type, DecimalType, $deci_f_f },
      { DecimalType, Float64Type, DecimalType, $deci_f_f },
      { Int16Type, DecimalType, DecimalType, $deci_f },
      { Int32Type, DecimalType, DecimalType, $deci_f },
      { Int64Type, DecimalType, DecimalType, $deci_f },
      { DecimalType, DecimalType, DecimalType, $deci_f },
      { Float32Type, DecimalType, DecimalType, $deci_f_f },
      { Float64Type, DecimalType, DecimalType, $deci_f_f },
      { TimestampType, TimestampType, TimestampType, $int_f },
      { DateType, DateType, Int32Type, $int_f },
      { DateType, IntervalType, TimestampType, $date_interval_f },
      { IntervalType, DateType, TimestampType, $interval_date_f }
    }
  };
}

pub fn new_binary_expr(
    expr_type: ExprNode_Type,
    ret: DataTypeRef,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        ExprNode_Type::EQUAL => {
            gen_binary_expr! {comparison_impl, prim_eq, prim_eq, deci_f_eq, deci_eq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ExprNode_Type::NOT_EQUAL => {
            gen_binary_expr! {comparison_impl, prim_neq, prim_neq, deci_f_neq, deci_neq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ExprNode_Type::LESS_THAN => {
            gen_binary_expr! {comparison_impl, prim_lt, prim_lt, deci_f_lt, deci_lt, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ExprNode_Type::GREATER_THAN => {
            gen_binary_expr! {comparison_impl, prim_gt, prim_gt, deci_f_gt, deci_gt, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ExprNode_Type::GREATER_THAN_OR_EQUAL => {
            gen_binary_expr! {comparison_impl, prim_geq, prim_geq, deci_f_geq, deci_geq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ExprNode_Type::LESS_THAN_OR_EQUAL => {
            gen_binary_expr! {comparison_impl, prim_leq, prim_leq, deci_f_leq, deci_leq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ExprNode_Type::ADD => {
            gen_binary_expr! {arithmetic_impl, int_add, float_add, deci_f_add, deci_add, interval_date_add, date_interval_add, l, r, ret}
        }
        ExprNode_Type::SUBTRACT => {
            gen_binary_expr! {arithmetic_impl, int_sub, float_sub, deci_f_sub, deci_sub, atm_placeholder, date_interval_sub, l, r, ret}
        }
        ExprNode_Type::MULTIPLY => {
            gen_binary_expr! {arithmetic_impl, int_mul, float_mul, deci_f_mul, deci_mul, atm_placeholder, atm_placeholder, l, r, ret}
        }
        ExprNode_Type::DIVIDE => {
            gen_binary_expr! {arithmetic_impl, int_div, float_div, deci_f_div, deci_div, atm_placeholder, atm_placeholder, l, r, ret}
        }
        ExprNode_Type::MODULUS => {
            gen_binary_expr! {arithmetic_impl, prim_mod, prim_mod, deci_f_mod, deci_mod, atm_placeholder, atm_placeholder, l, r, ret}
        }
        ExprNode_Type::AND => Box::new(BinaryExpression::<BoolArray, BoolArray, BoolArray, _> {
            expr_ia1: l,
            expr_ia2: r,
            return_type: ret,
            func: and,
            _phantom: PhantomData,
        }),
        ExprNode_Type::OR => Box::new(BinaryExpression::<BoolArray, BoolArray, BoolArray, _> {
            expr_ia1: l,
            expr_ia2: r,
            return_type: ret,
            func: or,
            _phantom: PhantomData,
        }),
        _ => {
            unimplemented!(
                "The expression using vectorized expression framework is not supported yet!"
            )
        }
    }
}

pub fn new_like_default(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryExpression::<UTF8Array, UTF8Array, BoolArray, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: like_default,
        _phantom: PhantomData,
    })
}

pub fn new_position_expr(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryExpression::<UTF8Array, UTF8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: position,
        _phantom: PhantomData,
    })
}
