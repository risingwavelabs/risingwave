/// For expression that only accept two non null arguments as input.
use crate::array::{Array, BoolArray, DataTypeTrait, I32Array, UTF8Array};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::template::BinaryExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::types::*;
use crate::vector_op::arithmetic_op::*;
use crate::vector_op::cmp::*;
use crate::vector_op::conjunction::{and, or};
use crate::vector_op::like::like_default;
use crate::vector_op::position::position;
use risingwave_pb::expr::expr_node::Type as ProstExprType;
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
/// * $func: The scalar function for expression, it's a generic function and specialized by the type
///   of `$i1, $i2, $cast`
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
/// * $func: The scalar function for expression, it's a generic function and specialized by the type
///   of `$i1, $i2, $cast`
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
/// Specifically, the first type is left input, the second type is right input and the third is the
/// cast type For different data type, the scalar function may be different. Therefore we need to
/// pass all possible scalar function
/// * `macro`: a macro helps create expression
/// * `int_f`: the scalar function of integer
/// * `float_f`: the scalar function of float
/// * `deci_f`: the scalar function for decimal with integer. In this scalar function, all inputs
///   will be cast to decimal
/// * `deci_f_f`: the scalar function for decimal with float. In this scalar function, all inputs
///   will be cast to float
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
    expr_type: ProstExprType,
    ret: DataTypeRef,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        ProstExprType::Equal => {
            gen_binary_expr! {comparison_impl, prim_eq, prim_eq, deci_f_eq, deci_eq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ProstExprType::NotEqual => {
            gen_binary_expr! {comparison_impl, prim_neq, prim_neq, deci_f_neq, deci_neq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ProstExprType::LessThan => {
            gen_binary_expr! {comparison_impl, prim_lt, prim_lt, deci_f_lt, deci_lt, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ProstExprType::GreaterThan => {
            gen_binary_expr! {comparison_impl, prim_gt, prim_gt, deci_f_gt, deci_gt, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ProstExprType::GreaterThanOrEqual => {
            gen_binary_expr! {comparison_impl, prim_geq, prim_geq, deci_f_geq, deci_geq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ProstExprType::LessThanOrEqual => {
            gen_binary_expr! {comparison_impl, prim_leq, prim_leq, deci_f_leq, deci_leq, cmp_placeholder, cmp_placeholder, l, r, ret}
        }
        ProstExprType::Add => {
            gen_binary_expr! {arithmetic_impl, int_add, float_add, deci_f_add, deci_add, interval_date_add, date_interval_add, l, r, ret}
        }
        ProstExprType::Subtract => {
            gen_binary_expr! {arithmetic_impl, int_sub, float_sub, deci_f_sub, deci_sub, atm_placeholder, date_interval_sub, l, r, ret}
        }
        ProstExprType::Multiply => {
            gen_binary_expr! {arithmetic_impl, int_mul, float_mul, deci_f_mul, deci_mul, atm_placeholder, atm_placeholder, l, r, ret}
        }
        ProstExprType::Divide => {
            gen_binary_expr! {arithmetic_impl, int_div, float_div, deci_f_div, deci_div, atm_placeholder, atm_placeholder, l, r, ret}
        }
        ProstExprType::Modulus => {
            gen_binary_expr! {arithmetic_impl, prim_mod, prim_mod, deci_f_mod, deci_mod, atm_placeholder, atm_placeholder, l, r, ret}
        }
        ProstExprType::And => Box::new(BinaryExpression::<BoolArray, BoolArray, BoolArray, _> {
            expr_ia1: l,
            expr_ia2: r,
            return_type: ret,
            func: and,
            _phantom: PhantomData,
        }),
        ProstExprType::Or => Box::new(BinaryExpression::<BoolArray, BoolArray, BoolArray, _> {
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

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::array::column::Column;
    use crate::array::interval_array::IntervalArray;
    use crate::array::*;
    use crate::expr::test_utils::make_expression;
    use crate::types::{
        BoolType, DateType, DecimalType, Int32Type, IntervalType, IntervalUnit, Scalar,
    };
    use crate::vector_op::arithmetic_op::{date_interval_add, date_interval_sub};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::expr_node::Type as ProstExprType;
    use rust_decimal::Decimal;

    #[test]
    fn test_binary() {
        test_binary_i32::<I32Array, _>(|x, y| x + y, ProstExprType::Add);
        test_binary_i32::<I32Array, _>(|x, y| x - y, ProstExprType::Subtract);
        test_binary_i32::<I32Array, _>(|x, y| x * y, ProstExprType::Multiply);
        test_binary_i32::<I32Array, _>(|x, y| x / y, ProstExprType::Divide);
        test_binary_i32::<BoolArray, _>(|x, y| x == y, ProstExprType::Equal);
        test_binary_i32::<BoolArray, _>(|x, y| x != y, ProstExprType::NotEqual);
        test_binary_i32::<BoolArray, _>(|x, y| x > y, ProstExprType::GreaterThan);
        test_binary_i32::<BoolArray, _>(|x, y| x >= y, ProstExprType::GreaterThanOrEqual);
        test_binary_i32::<BoolArray, _>(|x, y| x < y, ProstExprType::LessThan);
        test_binary_i32::<BoolArray, _>(|x, y| x <= y, ProstExprType::LessThanOrEqual);
        test_binary_decimal::<DecimalArray, _>(|x, y| x + y, ProstExprType::Add);
        test_binary_decimal::<DecimalArray, _>(|x, y| x - y, ProstExprType::Subtract);
        test_binary_decimal::<DecimalArray, _>(|x, y| x * y, ProstExprType::Multiply);
        test_binary_decimal::<DecimalArray, _>(|x, y| x / y, ProstExprType::Divide);
        test_binary_decimal::<BoolArray, _>(|x, y| x == y, ProstExprType::Equal);
        test_binary_decimal::<BoolArray, _>(|x, y| x != y, ProstExprType::NotEqual);
        test_binary_decimal::<BoolArray, _>(|x, y| x > y, ProstExprType::GreaterThan);
        test_binary_decimal::<BoolArray, _>(|x, y| x >= y, ProstExprType::GreaterThanOrEqual);
        test_binary_decimal::<BoolArray, _>(|x, y| x < y, ProstExprType::LessThan);
        test_binary_decimal::<BoolArray, _>(|x, y| x <= y, ProstExprType::LessThanOrEqual);
        test_binary_bool::<BoolArray, _>(|x, y| x && y, ProstExprType::And);
        test_binary_bool::<BoolArray, _>(|x, y| x || y, ProstExprType::Or);
        test_binary_interval::<I64Array, _>(
            |x, y| date_interval_add::<i32, i32, i64>(x, y).unwrap(),
            ProstExprType::Add,
        );
        test_binary_interval::<I64Array, _>(
            |x, y| date_interval_sub::<i32, i32, i64>(x, y).unwrap(),
            ProstExprType::Subtract,
        );
    }

    fn test_binary_i32<A, F>(f: F, kind: ProstExprType)
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
            Int32Type::create(true),
        );
        let col2 = Column::new(
            I32Array::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
            Int32Type::create(true),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Int32, TypeName::Int32], &[0, 1]);
        let mut vec_excutor = build_from_prost(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_binary_interval<A, F>(f: F, kind: ProstExprType)
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
            DateType::create(true),
        );
        let col2 = Column::new(
            IntervalArray::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
            IntervalType::create(true),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Date, TypeName::Interval], &[0, 1]);
        let mut vec_excutor = build_from_prost(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_binary_decimal<A, F>(f: F, kind: ProstExprType)
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
            DecimalType::create(true, 10, 5).unwrap(),
        );
        let col2 = Column::new(
            DecimalArray::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
            DecimalType::create(true, 10, 5).unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Decimal, TypeName::Decimal], &[0, 1]);
        let mut vec_excutor = build_from_prost(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_binary_bool<A, F>(f: F, kind: ProstExprType)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(bool, bool) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<bool>>::new();
        let mut rhs = Vec::<Option<bool>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                lhs.push(Some(true));
                rhs.push(None);
                target.push(None);
            } else if i % 3 == 0 {
                lhs.push(Some(true));
                rhs.push(Some(false));
                target.push(Some(f(true, false)));
            } else if i % 5 == 0 {
                lhs.push(Some(false));
                rhs.push(Some(false));
                target.push(Some(f(false, false)));
            } else {
                lhs.push(Some(true));
                rhs.push(Some(true));
                target.push(Some(f(true, true)));
            }
        }

        let col1 = Column::new(
            BoolArray::from_slice(&lhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
            BoolType::create(true),
        );
        let col2 = Column::new(
            BoolArray::from_slice(&rhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
            BoolType::create(true),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind, &[TypeName::Decimal, TypeName::Decimal], &[0, 1]);
        let mut vec_excutor = build_from_prost(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }
}
