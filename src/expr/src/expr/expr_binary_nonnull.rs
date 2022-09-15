// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::{
    Array, BoolArray, DecimalArray, I32Array, IntervalArray, ListArray, NaiveDateArray,
    NaiveDateTimeArray, StructArray, Utf8Array,
};
use risingwave_common::types::*;
use risingwave_pb::expr::expr_node::Type;

use crate::expr::expr_binary_bytes::new_concat_op;
use crate::expr::template::BinaryExpression;
use crate::expr::BoxedExpression;
use crate::for_all_cmp_variants;
use crate::vector_op::arithmetic_op::*;
use crate::vector_op::bitwise_op::*;
use crate::vector_op::cmp::*;
use crate::vector_op::extract::{extract_from_date, extract_from_timestamp};
use crate::vector_op::like::like_default;
use crate::vector_op::position::position;
use crate::vector_op::round::round_digits;
use crate::vector_op::tumble::{tumble_start_date, tumble_start_date_time};

/// This macro helps create arithmetic expression.
/// It receive all the combinations of `gen_binary_expr` and generate corresponding match cases
/// In [], the parameters are for constructing new expression
/// * $l: left expression
/// * $r: right expression
/// * $ret: return array type
/// In ()*, the parameters are for generating match cases
/// * $i1: left array type
/// * $i2: right array type
/// * $rt: The return type in that the operation will calculate
/// * $The scalar function for expression, it's a generic function and specialized by the type of
///   `$i1, $i2, $rt`
macro_rules! gen_atm_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $rt:ident, $func:ident },)*) => {
        match ($l.return_type(), $r.return_type()) {
            $(
                ($i1! { type_match_pattern }, $i2! { type_match_pattern }) => {
                    Box::new(
                        BinaryExpression::<
                            $i1! { type_array },
                            $i2! { type_array },
                            $rt! { type_array },
                            _
                        >::new(
                            $l,
                            $r,
                            $ret,
                            $func::< <$i1! { type_array } as Array>::OwnedItem, <$i2! { type_array } as Array>::OwnedItem, <$rt! { type_array } as Array>::OwnedItem>,
                        )
                    )
                },
            )*
            _ => {
                unimplemented!("The expression ({:?}, {:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type(), $r.return_type(), $ret)
            }
        }
    };
}

/// This macro helps create comparison expression. Its output array is a bool array
/// Similar to `gen_atm_impl`.
macro_rules! gen_cmp_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $cast:ident, $func:ident} ),* $(,)?) => {
        match ($l.return_type(), $r.return_type()) {
            $(
                ($i1! { type_match_pattern }, $i2! { type_match_pattern }) => {
                    Box::new(
                        BinaryExpression::<
                            $i1! { type_array },
                            $i2! { type_array },
                            BoolArray,
                            _
                        >::new(
                            $l,
                            $r,
                            $ret,
                            $func::<
                                <$i1! { type_array } as Array>::OwnedItem,
                                <$i2! { type_array } as Array>::OwnedItem,
                                <$cast! { type_array } as Array>::OwnedItem
                            >,
                        )
                    )
                }
            ),*
            _ => {
                unimplemented!("The expression ({:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type(), $r.return_type())
            }
        }
    };
}

/// This macro helps create bitwise shift expression. The Output type is same as LHS of the
/// expression and the RHS of the expression is being match into u32. Similar to `gen_atm_impl`.
macro_rules! gen_shift_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $func:ident },)*) => {
        match ($l.return_type(), $r.return_type()) {
            $(
                ($i1! { type_match_pattern }, $i2! { type_match_pattern }) => {
                    Box::new(
                        BinaryExpression::<
                            $i1! { type_array },
                            $i2! { type_array },
                            $i1! { type_array },
                            _
                        >::new(
                            $l,
                            $r,
                            $ret,
                            $func::<
                                <$i1! { type_array } as Array>::OwnedItem,
                                <$i2! { type_array } as Array>::OwnedItem>,

                        )
                    )
                },
            )*
            _ => {
                unimplemented!("The expression ({:?}, {:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type(), $r.return_type(), $ret)
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
    ($macro:ident, $general_f:ident, $op:ident, $l:expr, $r:expr, $ret:expr) => {
        match ($l.return_type(), $r.return_type()) {
            (DataType::Varchar, DataType::Varchar) => {
                Box::new(BinaryExpression::<Utf8Array, Utf8Array, BoolArray, _>::new(
                    $l,
                    $r,
                    $ret,
                    gen_str_cmp($op),
                ))
            }
            (DataType::Struct { .. }, DataType::Struct { .. }) => {
                Box::new(
                    BinaryExpression::<StructArray, StructArray, BoolArray, _>::new(
                        $l,
                        $r,
                        $ret,
                        gen_struct_cmp($op),
                    ),
                )
            }
            (DataType::List { .. }, DataType::List { .. }) => {
                Box::new(BinaryExpression::<ListArray, ListArray, BoolArray, _>::new(
                    $l,
                    $r,
                    $ret,
                    gen_list_cmp($op),
                ))
            }
            _ => {
                for_all_cmp_variants! {$macro, $l, $r, $ret, $general_f}
            }
        }
    };
}

/// `gen_binary_expr_atm` is similar to `gen_binary_expr_cmp`.
///  `atm` means arithmetic here.
/// They are differentiate cuz one type may not support atm and cmp at the same time. For example,
/// Varchar can support compare but not arithmetic.
/// * `$general_f`: generic atm function (require a common ``TryInto`` type for two input)
/// * `$i1`, `$i2`, `$rt`, `$func`: extra list passed to `$macro` directly
macro_rules! gen_binary_expr_atm {
    (
        $macro:ident,
        $l:expr,
        $r:expr,
        $ret:expr,
        $general_f:ident,
        {
            $( { $i1:ident, $i2:ident, $rt:ident, $func:ident }, )*
        } $(,)?
    ) => {
        $macro! {
            [$l, $r, $ret],
            { int16, int16, int16, $general_f },
            { int16, int32, int32, $general_f },
            { int16, int64, int64, $general_f },
            { int16, float32, float64, $general_f },
            { int16, float64, float64, $general_f },
            { int32, int16, int32, $general_f },
            { int32, int32, int32, $general_f },
            { int32, int64, int64, $general_f },
            { int32, float32, float64, $general_f },
            { int32, float64, float64, $general_f },
            { int64, int16,int64, $general_f },
            { int64, int32,int64, $general_f },
            { int64, int64, int64, $general_f },
            { int64, float32, float64 , $general_f},
            { int64, float64, float64, $general_f },
            { float32, int16, float64, $general_f },
            { float32, int32, float64, $general_f },
            { float32, int64, float64 , $general_f},
            { float32, float32, float32, $general_f },
            { float32, float64, float64, $general_f },
            { float64, int16, float64, $general_f },
            { float64, int32, float64, $general_f },
            { float64, int64, float64, $general_f },
            { float64, float32, float64, $general_f },
            { float64, float64, float64, $general_f },
            { decimal, int16, decimal, $general_f }, // decimal + int16 = decimal
            { decimal, int32, decimal, $general_f },
            { decimal, int64, decimal, $general_f },
            { decimal, float32, decimal, $general_f },
            { decimal, float64, decimal, $general_f },
            { int16, decimal, decimal, $general_f },
            { int32, decimal, decimal, $general_f },
            { int64, decimal, decimal, $general_f },
            { decimal, decimal, decimal, $general_f },
            { float32, decimal, float64, $general_f },
            { float64, decimal, float64, $general_f },
            $(
                { $i1, $i2, $rt, $func },
            )*
        }
    };
}

/// `gen_binary_expr_bitwise` is similar to `gen_binary_expr_atm`.
/// They are differentiate because bitwise operation only supports integral datatype.
/// * `$general_f`: generic atm function (require a common ``TryInto`` type for two input)
/// * `$i1`, `$i2`, `$rt`, `$func`: extra list passed to `$macro` directly
macro_rules! gen_binary_expr_bitwise {
    (
        $macro:ident,
        $l:expr,
        $r:expr,
        $ret:expr,
        $general_f:ident,
        {
            $( { $i1:ident, $i2:ident, $rt:ident, $func:ident }, )*
        } $(,)?
    ) => {
        $macro! {
            [$l, $r, $ret],
            { int16, int16, int16, $general_f },
            { int16, int32, int32, $general_f },
            { int16, int64, int64, $general_f },
            { int32, int16, int32, $general_f },
            { int32, int32, int32, $general_f },
            { int32, int64, int64, $general_f },
            { int64, int16,int64, $general_f },
            { int64, int32,int64, $general_f },
            { int64, int64, int64, $general_f },
            $(
                { $i1, $i2, $rt, $func },
            )*
        }
    };
}

/// `gen_binary_expr_shift` is similar to `gen_binary_expr_bitwise`.
/// They are differentiate because shift operation have different typing rules.
/// * `$general_f`: generic atm function
/// `$rt` is not required because Type of the output is same as the Type of LHS of expression.
/// * `$i1`, `$i2`, `$func`: extra list passed to `$macro` directly
macro_rules! gen_binary_expr_shift {
    (
        $macro:ident,
        $l:expr,
        $r:expr,
        $ret:expr,
        $general_f:ident,
        {
            $( { $i1:ident, $i2:ident, $func:ident }, )*
        } $(,)?
    ) => {
        $macro! {
            [$l, $r, $ret],
            { int16, int16, $general_f },
            { int32, int16, $general_f },
            { int16, int32, $general_f },
            { int32, int32, $general_f },
            { int64, int16, $general_f },
            { int64, int32, $general_f },
            $(
                { $i1, $i2, $func },
            )*
        }
    };
}

fn build_extract_expr(ret: DataType, l: BoxedExpression, r: BoxedExpression) -> BoxedExpression {
    match r.return_type() {
        DataType::Date => Box::new(
            BinaryExpression::<Utf8Array, NaiveDateArray, DecimalArray, _>::new(
                l,
                r,
                ret,
                extract_from_date,
            ),
        ),
        DataType::Timestamp => Box::new(BinaryExpression::<
            Utf8Array,
            NaiveDateTimeArray,
            DecimalArray,
            _,
        >::new(l, r, ret, extract_from_timestamp)),
        _ => {
            unimplemented!("Extract ( {:?} ) is not supported yet!", r.return_type())
        }
    }
}

pub fn new_binary_expr(
    expr_type: Type,
    ret: DataType,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    use crate::expr::data_types::*;
    match expr_type {
        Type::Equal => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_eq, EQ, l, r, ret}
        }
        Type::NotEqual => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_ne, NE, l, r, ret}
        }
        Type::LessThan => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_lt, LT, l, r, ret}
        }
        Type::GreaterThan => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_gt, GT, l, r, ret}
        }
        Type::GreaterThanOrEqual => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_ge, GE, l, r, ret}
        }
        Type::LessThanOrEqual => {
            gen_binary_expr_cmp! {gen_cmp_impl, general_le, LE, l, r, ret}
        }
        Type::Add => {
            gen_binary_expr_atm! {
                gen_atm_impl,
                l, r, ret,
                general_add,
                {
                    { timestamp, interval, timestamp, timestamp_interval_add },
                    { interval, timestamp, timestamp, interval_timestamp_add },
                    { interval, date, timestamp, interval_date_add },
                    { interval, time, time, interval_time_add },
                    { date, interval, timestamp, date_interval_add },
                    { date, int32, date, date_int_add },
                    { int32, date, date, int_date_add },
                    { date, time, timestamp, date_time_add },
                    { time, date, timestamp, time_date_add },
                    { interval, interval, interval, general_add },
                    { time, interval, time, time_interval_add },
                },
            }
        }
        Type::Subtract => {
            gen_binary_expr_atm! {
                gen_atm_impl,
                l, r, ret,
                general_sub,
                {
                    { timestamp, timestamp, interval, timestamp_timestamp_sub },
                    { timestamp, interval, timestamp, timestamp_interval_sub },
                    { date, date, int32, date_date_sub },
                    { date, interval, timestamp, date_interval_sub },
                    { time, time, interval, time_time_sub },
                    { time, interval, time, time_interval_sub },
                    { interval, interval, interval, general_sub },
                    { date, int32, date, date_int_sub },
                },
            }
        }
        Type::Multiply => {
            gen_binary_expr_atm! {
                gen_atm_impl,
                l, r, ret,
                general_mul,
                {
                    { interval, int16, interval, interval_int_mul },
                    { interval, int32, interval, interval_int_mul },
                    { interval, int64, interval, interval_int_mul },
                    { interval, float32, interval, interval_float_mul },
                    { interval, float64, interval, interval_float_mul },
                    { interval, decimal, interval, interval_float_mul },

                    { int16, interval, interval, int_interval_mul },
                    { int32, interval, interval, int_interval_mul },
                    { int64, interval, interval, int_interval_mul },
                    { float32, interval, interval, float_interval_mul },
                    { float64, interval, interval, float_interval_mul },
                    { decimal, interval, interval, float_interval_mul },
                },
            }
        }
        Type::Divide => {
            gen_binary_expr_atm! {
                gen_atm_impl,
                l, r, ret,
                general_div,
                {
                    { interval, int16, interval, interval_float_div },
                    { interval, int32, interval, interval_float_div },
                    { interval, int64, interval, interval_float_div },
                    { interval, float32, interval, interval_float_div },
                    { interval, float64, interval, interval_float_div },
                    { interval, decimal, interval, interval_float_div },
                },
            }
        }
        Type::Modulus => {
            gen_binary_expr_atm! {
                gen_atm_impl,
                l, r, ret,
                general_mod,
                {
                },
            }
        }
        // BitWise Operation
        Type::BitwiseShiftLeft => {
            gen_binary_expr_shift! {
                gen_shift_impl,
                l, r, ret,
                general_shl,
                {

                },
            }
        }
        Type::BitwiseShiftRight => {
            gen_binary_expr_shift! {
                gen_shift_impl,
                l, r, ret,
                general_shr,
                {

                },
            }
        }
        Type::BitwiseAnd => {
            gen_binary_expr_bitwise! {
                gen_atm_impl,
                l, r, ret,
                general_bitand,
                {
                },
            }
        }
        Type::BitwiseOr => {
            gen_binary_expr_bitwise! {
                gen_atm_impl,
                l, r, ret,
                general_bitor,
                {
                },
            }
        }
        Type::BitwiseXor => {
            gen_binary_expr_bitwise! {
                gen_atm_impl,
                l, r, ret,
                general_bitxor,
                {
                },
            }
        }
        Type::Extract => build_extract_expr(ret, l, r),
        Type::RoundDigit => Box::new(
            BinaryExpression::<DecimalArray, I32Array, DecimalArray, _>::new(
                l,
                r,
                ret,
                round_digits,
            ),
        ),
        Type::Position => Box::new(BinaryExpression::<Utf8Array, Utf8Array, I32Array, _>::new(
            l, r, ret, position,
        )),
        Type::TumbleStart => new_tumble_start(l, r, ret),
        Type::ConcatOp => new_concat_op(l, r, ret),

        tp => {
            unimplemented!(
                "The expression {:?} using vectorized expression framework is not supported yet!",
                tp
            )
        }
    }
}

fn new_tumble_start(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    match expr_ia1.return_type() {
        DataType::Date => Box::new(BinaryExpression::<
            NaiveDateArray,
            IntervalArray,
            NaiveDateTimeArray,
            _,
        >::new(
            expr_ia1, expr_ia2, return_type, tumble_start_date
        )),
        DataType::Timestamp => Box::new(BinaryExpression::<
            NaiveDateTimeArray,
            IntervalArray,
            NaiveDateTimeArray,
            _,
        >::new(
            expr_ia1, expr_ia2, return_type, tumble_start_date_time
        )),
        _ => unimplemented!(
            "tumble_start is not supported for {:?}",
            expr_ia1.return_type()
        ),
    }
}

pub fn new_like_default(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    Box::new(BinaryExpression::<Utf8Array, Utf8Array, BoolArray, _>::new(
        expr_ia1,
        expr_ia2,
        return_type,
        like_default,
    ))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::interval_array::IntervalArray;
    use risingwave_common::array::*;
    use risingwave_common::types::{
        Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, Scalar,
    };
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::expr_node::Type;

    use super::super::*;
    use crate::expr::test_utils::make_expression;
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
        test_binary_interval::<NaiveDateTimeArray, _>(
            |x, y| {
                date_interval_add::<NaiveDateWrapper, IntervalUnit, NaiveDateTimeWrapper>(x, y)
                    .unwrap()
            },
            Type::Add,
        );
        test_binary_interval::<NaiveDateTimeArray, _>(
            |x, y| {
                date_interval_sub::<NaiveDateWrapper, IntervalUnit, NaiveDateTimeWrapper>(x, y)
                    .unwrap()
            },
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

        let col1 = Column::new(Arc::new(I32Array::from_slice(&lhs).into()));
        let col2 = Column::new(Arc::new(I32Array::from_slice(&rhs).into()));
        let data_chunk = DataChunk::new(vec![col1, col2], 100);
        let expr = make_expression(kind, &[TypeName::Int32, TypeName::Int32], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|int| int.to_scalar_value()),
                rhs[i].map(|int| int.to_scalar_value()),
            ]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    fn test_binary_interval<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(NaiveDateWrapper, IntervalUnit) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<NaiveDateWrapper>>::new();
        let mut rhs = Vec::<Option<IntervalUnit>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
                lhs.push(None);
                target.push(None);
            } else {
                rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
                lhs.push(Some(NaiveDateWrapper::new(
                    NaiveDate::from_num_days_from_ce(i),
                )));
                target.push(Some(f(
                    NaiveDateWrapper::new(NaiveDate::from_num_days_from_ce(i)),
                    IntervalUnit::from_ymd(0, i, i),
                )));
            }
        }

        let col1 = Column::new(Arc::new(NaiveDateArray::from_slice(&lhs).into()));
        let col2 = Column::new(Arc::new(IntervalArray::from_slice(&rhs).into()));
        let data_chunk = DataChunk::new(vec![col1, col2], 100);
        let expr = make_expression(kind, &[TypeName::Date, TypeName::Interval], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|date| date.to_scalar_value()),
                rhs[i].map(|date| date.to_scalar_value()),
            ]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
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

        let col1 = Column::new(Arc::new(DecimalArray::from_slice(&lhs).into()));
        let col2 = Column::new(Arc::new(DecimalArray::from_slice(&rhs).into()));
        let data_chunk = DataChunk::new(vec![col1, col2], 100);
        let expr = make_expression(kind, &[TypeName::Decimal, TypeName::Decimal], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|dec| dec.to_scalar_value()),
                rhs[i].map(|dec| dec.to_scalar_value()),
            ]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }
}
