use std::marker::PhantomData;

use risingwave_pb::expr::expr_node::Type;

use crate::array::{Array, BoolArray, DecimalArray, I32Array, I64Array, Utf8Array};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::data_types::*;
use crate::expr::template::BinaryExpression;
use crate::expr::BoxedExpression;
use crate::types::*;
use crate::vector_op::arithmetic_op::*;
use crate::vector_op::cmp::*;
use crate::vector_op::extract::{extract_from_date, extract_from_timestamp};
use crate::vector_op::like::like_default;
use crate::vector_op::position::position;
use crate::vector_op::round::round_digits;

/// A placeholder function that return bool in [`gen_binary_expr_atm`]
pub fn cmp_placeholder<T1, T2, T3>(_l: T1, _r: T2) -> Result<bool> {
    Err(InternalError("The function is not supported".to_string()).into())
}

/// A placeholder function that return T3 in [`gen_binary_expr_atm`]
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
/// * $func: The scalar function for expression, it's a generic function and specialized by the type
///   of `$i1, $i2, $cast`
macro_rules! gen_atm_impl {
  ([$l:expr, $r:expr, $ret:expr], $( { $i1:ty, $i2:ty, $cast:ty, $func:ident} ),*) => {
    match ($l.return_type(), $r.return_type()) {
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
        unimplemented!("The expression ({:?}, {:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type(), $r.return_type(), $ret)
      }
    }
  };
}

/// This macro helps create comparison expression. Its output array is a bool array
/// Similar to `gen_atm_impl`.
macro_rules! gen_cmp_impl {
  ([$l:expr, $r:expr, $ret:expr], $( { $i1:ty, $i2:ty, $cast:ty, $func: ident} ),*) => {
    match ($l.return_type(), $r.return_type()) {
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
        unimplemented!("The expression ({:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type(), $r.return_type())
      }
    }
  };
}

/// Based on the data type of `$l`, `$r`, `$ret`, return corresponding expression struct with scalar
/// function inside.
/// * `$l`: left expression
/// * `$r`: right expression
/// * `$ret`: returned expression
/// * `macro`: a macro helps create expression
/// * `general_f`: generic cmp function (require a common ``TryInto`` type for two input).
/// * `str_f`: cmp function between str
macro_rules! gen_binary_expr_cmp {
    ($macro:tt, $general_f:ident, $str_f:ident, $l:expr, $r:expr, $ret:expr) => {
        match ($l.return_type(), $r.return_type()) {
            (DataTypeKind::Varchar, DataTypeKind::Varchar) => {
                Box::new(BinaryExpression::<Utf8Array, Utf8Array, BoolArray, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $str_f,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Varchar, DataTypeKind::Char) => {
                Box::new(BinaryExpression::<Utf8Array, Utf8Array, BoolArray, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $str_f,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Char, DataTypeKind::Char) => {
                Box::new(BinaryExpression::<Utf8Array, Utf8Array, BoolArray, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $str_f,
                    _phantom: PhantomData,
                })
            }
            _ => {
                $macro! {
                      [$l, $r, $ret],
                      { Int16Type, Int16Type, Int16Type, $general_f },
                      { Int16Type, Int32Type, Int32Type, $general_f },
                      { Int16Type, Int64Type, Int64Type, $general_f },
                      { Int16Type, Float32Type, Float64Type, $general_f },
                      { Int16Type, Float64Type, Float64Type, $general_f },
                      { Int32Type, Int16Type, Int32Type, $general_f },
                      { Int32Type, Int32Type, Int32Type, $general_f },
                      { Int32Type, Int64Type, Int64Type, $general_f },
                      { Int32Type, Float32Type, Float64Type, $general_f },
                      { Int32Type, Float64Type, Float64Type, $general_f },
                      { Int64Type, Int16Type,Int64Type, $general_f },
                      { Int64Type, Int32Type,Int64Type, $general_f },
                      { Int64Type, Int64Type, Int64Type, $general_f },
                      { Int64Type, Float32Type, Float64Type , $general_f},
                      { Int64Type, Float64Type, Float64Type, $general_f },
                      { Float32Type, Int16Type, Float64Type, $general_f },
                      { Float32Type, Int32Type, Float64Type, $general_f },
                      { Float32Type, Int64Type, Float64Type , $general_f},
                      { Float32Type, Float32Type, Float32Type, $general_f },
                      { Float32Type, Float64Type, Float64Type, $general_f },
                      { Float64Type, Int16Type, Float64Type, $general_f },
                      { Float64Type, Int32Type, Float64Type, $general_f },
                      { Float64Type, Int64Type, Float64Type, $general_f },
                      { Float64Type, Float32Type, Float64Type, $general_f },
                      { Float64Type, Float64Type, Float64Type, $general_f },
                      { DecimalType, Int16Type, DecimalType, $general_f },
                      { DecimalType, Int32Type, DecimalType, $general_f },
                      { DecimalType, Int64Type, DecimalType, $general_f },
                      { DecimalType, Float32Type, Float64Type, $general_f },
                      { DecimalType, Float64Type, Float64Type, $general_f },
                      { Int16Type, DecimalType, DecimalType, $general_f },
                      { Int32Type, DecimalType, DecimalType, $general_f },
                      { Int64Type, DecimalType, DecimalType, $general_f },
                      { DecimalType, DecimalType, DecimalType, $general_f },
                      { Float32Type, DecimalType, Float64Type, $general_f },
                      { Float64Type, DecimalType, Float64Type, $general_f },
                      { TimestampType, TimestampType, TimestampType, $general_f },
                      { DateType, DateType, Int32Type, $general_f },
                      { BoolType, BoolType, BoolType, $general_f }
                }
            }
        }
    };
}

/// `gen_binary_expr_atm` is similar to `gen_binary_expr_cmp`.
///  `atm` means arithmetic here.
/// They are differentiate cuz one type may not support atm and cmp at the same time. For example,
/// Varchar can support compare but not arithmetic.
/// * `general_f`: generic atm function (require a common ``TryInto`` type for two input)
/// * `interval_date_f`: atm function between interval and date
/// * `interval_date_f`: atm function between date and interval
macro_rules! gen_binary_expr_atm {
    (
        $macro:tt,
        $general_f:ident,
        $interval_date_f:ident,
        $date_interval_f:ident,
        $l:expr,
        $r:expr,
        $ret:expr
    ) => {
        $macro! {
          [$l, $r, $ret],
          { Int16Type, Int16Type, Int16Type, $general_f },
          { Int16Type, Int32Type, Int32Type, $general_f },
          { Int16Type, Int64Type, Int64Type, $general_f },
          { Int16Type, Float32Type, Float64Type, $general_f },
          { Int16Type, Float64Type, Float64Type, $general_f },
          { Int32Type, Int16Type, Int32Type, $general_f },
          { Int32Type, Int32Type, Int32Type, $general_f },
          { Int32Type, Int64Type, Int64Type, $general_f },
          { Int32Type, Float32Type, Float64Type, $general_f },
          { Int32Type, Float64Type, Float64Type, $general_f },
          { Int64Type, Int16Type,Int64Type, $general_f },
          { Int64Type, Int32Type,Int64Type, $general_f },
          { Int64Type, Int64Type, Int64Type, $general_f },
          { Int64Type, Float32Type, Float64Type , $general_f},
          { Int64Type, Float64Type, Float64Type, $general_f },
          { Float32Type, Int16Type, Float64Type, $general_f },
          { Float32Type, Int32Type, Float64Type, $general_f },
          { Float32Type, Int64Type, Float64Type , $general_f},
          { Float32Type, Float32Type, Float32Type, $general_f },
          { Float32Type, Float64Type, Float64Type, $general_f },
          { Float64Type, Int16Type, Float64Type, $general_f },
          { Float64Type, Int32Type, Float64Type, $general_f },
          { Float64Type, Int64Type, Float64Type, $general_f },
          { Float64Type, Float32Type, Float64Type, $general_f },
          { Float64Type, Float64Type, Float64Type, $general_f },
          { DecimalType, Int16Type, DecimalType, $general_f },
          { DecimalType, Int32Type, DecimalType, $general_f },
          { DecimalType, Int64Type, DecimalType, $general_f },
          { DecimalType, Float32Type, DecimalType, $general_f },
          { DecimalType, Float64Type, DecimalType, $general_f },
          { Int16Type, DecimalType, DecimalType, $general_f },
          { Int32Type, DecimalType, DecimalType, $general_f },
          { Int64Type, DecimalType, DecimalType, $general_f },
          { DecimalType, DecimalType, DecimalType, $general_f },
          { Float32Type, DecimalType, Float64Type, $general_f },
          { Float64Type, DecimalType, Float64Type, $general_f },
          { TimestampType, TimestampType, TimestampType, $general_f },
          { DateType, DateType, Int32Type, $general_f },
          { DateType, IntervalType, TimestampType, $date_interval_f },
          { IntervalType, DateType, TimestampType, $interval_date_f }
        }
    };
}

fn build_extract_expr(
    ret: DataTypeKind,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match r.return_type() {
        DataTypeKind::Date => Box::new(BinaryExpression::<Utf8Array, I32Array, DecimalArray, _> {
            expr_ia1: l,
            expr_ia2: r,
            return_type: ret,
            func: extract_from_date,
            _phantom: PhantomData,
        }),
        DataTypeKind::Timestamp => {
            Box::new(BinaryExpression::<Utf8Array, I64Array, DecimalArray, _> {
                expr_ia1: l,
                expr_ia2: r,
                return_type: ret,
                func: extract_from_timestamp,
                _phantom: PhantomData,
            })
        }
        _ => {
            unimplemented!("Extract ( {:?} ) is not supported yet!", r.return_type())
        }
    }
}

pub fn new_binary_expr(
    expr_type: Type,
    ret: DataTypeKind,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        Type::Equal => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_eq, str_eq, l, r, ret}
        }
        Type::NotEqual => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_ne, str_ne, l, r, ret}
        }
        Type::LessThan => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_lt, str_lt, l, r, ret}
        }
        Type::GreaterThan => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_gt, str_gt, l, r, ret}
        }
        Type::GreaterThanOrEqual => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_ge, str_ge, l, r, ret}
        }
        Type::LessThanOrEqual => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_le, str_le, l, r, ret}
        }
        Type::Add => {
            gen_binary_expr_atm! {gen_atm_impl, general_add,
            interval_date_add, date_interval_add, l, r, ret}
        }
        Type::Subtract => {
            gen_binary_expr_atm! {gen_atm_impl, general_sub,
            atm_placeholder, date_interval_sub, l, r, ret}
        }
        Type::Multiply => {
            gen_binary_expr_atm! {gen_atm_impl, general_mul,
            atm_placeholder, atm_placeholder, l, r, ret}
        }
        Type::Divide => {
            gen_binary_expr_atm! {gen_atm_impl, general_div,
            atm_placeholder, atm_placeholder, l, r, ret}
        }
        Type::Modulus => {
            gen_binary_expr_atm! {gen_atm_impl, general_mod,
            atm_placeholder, atm_placeholder, l, r, ret}
        }
        Type::Extract => build_extract_expr(ret, l, r),
        Type::RoundDigit => Box::new(
            BinaryExpression::<DecimalArray, I32Array, DecimalArray, _> {
                expr_ia1: l,
                expr_ia2: r,
                return_type: ret,
                func: round_digits,
                _phantom: PhantomData,
            },
        ),
        tp => {
            unimplemented!(
                "The expression {:?} using vectorized expression framework is not supported yet!",
                tp
            )
        }
    }
}

pub fn new_like_default(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeKind,
) -> BoxedExpression {
    Box::new(BinaryExpression::<Utf8Array, Utf8Array, BoolArray, _> {
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
    return_type: DataTypeKind,
) -> BoxedExpression {
    Box::new(BinaryExpression::<Utf8Array, Utf8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: position,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::expr_node::Type;

    use super::super::*;
    use crate::array::column::Column;
    use crate::array::interval_array::IntervalArray;
    use crate::array::*;
    use crate::expr::test_utils::make_expression;
    use crate::types::{Decimal, IntervalUnit, Scalar};
    use crate::vector_op::arithmetic_op::{date_interval_add, date_interval_sub};

    #[test]
    fn test_binary() {
        test_binary_i32::<I32Array, _>(|x, y| x + y, Type::Add);
        test_binary_i32::<I32Array, _>(|x, y| x - y, Type::Subtract);
        test_binary_i32::<I32Array, _>(|x, y| x * y, Type::Multiply);
        test_binary_i32::<I32Array, _>(|x, y| x / y, Type::Divide);
        test_binary_i32::<BoolArray, _>(|x, y| x == y, Type::Equal);
        test_binary_i32::<BoolArray, _>(|x, y| x != y, Type::NotEqual);
        test_binary_i32::<BoolArray, _>(|x, y| x > y, Type::GreaterThan);
        test_binary_i32::<BoolArray, _>(|x, y| x >= y, Type::GreaterThanOrEqual);
        test_binary_i32::<BoolArray, _>(|x, y| x < y, Type::LessThan);
        test_binary_i32::<BoolArray, _>(|x, y| x <= y, Type::LessThanOrEqual);
        test_binary_decimal::<DecimalArray, _>(|x, y| x + y, Type::Add);
        test_binary_decimal::<DecimalArray, _>(|x, y| x - y, Type::Subtract);
        test_binary_decimal::<DecimalArray, _>(|x, y| x * y, Type::Multiply);
        test_binary_decimal::<DecimalArray, _>(|x, y| x / y, Type::Divide);
        test_binary_decimal::<BoolArray, _>(|x, y| x == y, Type::Equal);
        test_binary_decimal::<BoolArray, _>(|x, y| x != y, Type::NotEqual);
        test_binary_decimal::<BoolArray, _>(|x, y| x > y, Type::GreaterThan);
        test_binary_decimal::<BoolArray, _>(|x, y| x >= y, Type::GreaterThanOrEqual);
        test_binary_decimal::<BoolArray, _>(|x, y| x < y, Type::LessThan);
        test_binary_decimal::<BoolArray, _>(|x, y| x <= y, Type::LessThanOrEqual);
        test_binary_interval::<I64Array, _>(
            |x, y| date_interval_add::<i32, i32, i64>(x, y).unwrap(),
            Type::Add,
        );
        test_binary_interval::<I64Array, _>(
            |x, y| date_interval_sub::<i32, i32, i64>(x, y).unwrap(),
            Type::Subtract,
        );
    }

    fn test_binary_i32<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(i32, i32) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<i32>>::new();
        let mut rhs = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                lhs.push(Some(i));
                rhs.push(None);
                target.push(None);
            } else if i % 3 == 0 {
                lhs.push(Some(i));
                rhs.push(Some(i + 1));
                target.push(Some(f(i, i + 1)));
            } else if i % 5 == 0 {
                lhs.push(Some(i + 1));
                rhs.push(Some(i));
                target.push(Some(f(i + 1, i)));
            } else {
                lhs.push(Some(i));
                rhs.push(Some(i));
                target.push(Some(f(i, i)));
            }
        }

        let col1 = Column::new(
            I32Array::from_slice(&lhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let col2 = Column::new(
            I32Array::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Int32, TypeName::Int32], &[0, 1]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_binary_interval<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(i32, IntervalUnit) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<i32>>::new();
        let mut rhs = Vec::<Option<IntervalUnit>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
                lhs.push(None);
                target.push(None);
            } else {
                rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
                lhs.push(Some(i));
                target.push(Some(f(i, IntervalUnit::from_ymd(0, i, i))));
            }
        }

        let col1 = Column::new(
            I32Array::from_slice(&lhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let col2 = Column::new(
            IntervalArray::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Date, TypeName::Interval], &[0, 1]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_binary_decimal<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(Decimal, Decimal) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<Decimal>>::new();
        let mut rhs = Vec::<Option<Decimal>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                lhs.push(Some(i.into()));
                rhs.push(None);
                target.push(None);
            } else if i % 3 == 0 {
                lhs.push(Some(i.into()));
                rhs.push(Some((i + 1).into()));
                target.push(Some(f((i).into(), (i + 1).into())));
            } else if i % 5 == 0 {
                lhs.push(Some((i + 1).into()));
                rhs.push(Some((i).into()));
                target.push(Some(f((i + 1).into(), (i).into())));
            } else {
                lhs.push(Some((i).into()));
                rhs.push(Some((i).into()));
                target.push(Some(f((i).into(), (i).into())));
            }
        }

        let col1 = Column::new(
            DecimalArray::from_slice(&lhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let col2 = Column::new(
            DecimalArray::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Decimal, TypeName::Decimal], &[0, 1]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }
}
