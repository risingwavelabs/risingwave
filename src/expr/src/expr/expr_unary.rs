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

//! For expression that only accept one value as input (e.g. CAST)

use risingwave_common::array::*;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::*;
use risingwave_pb::expr::expr_node::Type as ProstType;

use super::template::{UnaryBytesExpression, UnaryExpression};
use crate::expr::expr_is_null::{IsNotNullExpression, IsNullExpression};
use crate::expr::pg_sleep::PgSleepExpression;
use crate::expr::template::UnaryNullableExpression;
use crate::expr::BoxedExpression;
use crate::vector_op::arithmetic_op::general_neg;
use crate::vector_op::ascii::ascii;
use crate::vector_op::cast::*;
use crate::vector_op::cmp::{is_false, is_not_false, is_not_true, is_true};
use crate::vector_op::conjunction;
use crate::vector_op::length::length_default;
use crate::vector_op::lower::lower;
use crate::vector_op::ltrim::ltrim;
use crate::vector_op::rtrim::rtrim;
use crate::vector_op::trim::trim;
use crate::vector_op::upper::upper;

/// This macro helps to create cast expression.
/// It receives all the combinations of `gen_cast` and generates corresponding match cases
/// In `[]`, the parameters are for constructing new expression
/// * `$child`: child expression
/// * `$ret`: return expression
///
/// In `()*`, the parameters are for generating match cases
/// * `$input`: input type
/// * `$cast`: The cast type in that the operation will calculate
/// * `$func`: The scalar function for expression, it's a generic function and specialized by the
///   type of `$input, $cast`
macro_rules! gen_cast_impl {
    ([$child:expr, $ret:expr], $( { $input:ident, $cast:ident, $func:expr } ),*) => {
        match ($child.return_type(), $ret.clone()) {
            $(
                ($input! { type_match_pattern }, $cast! { type_match_pattern }) => Box::new(
                    UnaryExpression::< $input! { type_array }, $cast! { type_array }, _>::new(
                        $child,
                        $ret.clone(),
                        $func
                    )
                ),
            )*
            _ => {
                return Err(ErrorCode::NotImplemented(format!(
                    "CAST({:?} AS {:?}) not supported yet!",
                    $child.return_type(), $ret
                ), 1632.into())
                .into());
            }
        }
    };
}

macro_rules! gen_cast {
    ($($x:tt, )* ) => {
        gen_cast_impl! {
            [$($x),*],

            // We do not always expect the frontend to do constant folding
            // to eliminate the unnecessary cast.
            { int16, int16, |x| Ok(x) },
            { int32, int32, |x| Ok(x) },
            { int64, int64, |x| Ok(x) },
            { float64, float64, |x| Ok(x) },
            { float32, float32, |x| Ok(x) },
            { decimal, decimal, |x| Ok(x) },
            { date, date, |x| Ok(x) },
            { timestamp, timestamp, |x| Ok(x) },
            { time, time, |x| Ok(x) },
            { boolean, boolean, |x| Ok(x) },
            { varchar, varchar, |x| Ok(x.into()) },

            { varchar, date, str_to_date },
            { varchar, time, str_to_time },
            { varchar, timestamp, str_to_timestamp },
            { varchar, timestampz, str_to_timestampz },
            { varchar, int16, str_parse },
            { varchar, int32, str_parse },
            { varchar, int64, str_parse },
            { varchar, float32, str_parse },
            { varchar, float64, str_parse },
            { varchar, decimal, str_parse },
            { varchar, boolean, str_to_bool },

            { boolean, varchar, bool_to_str },

            { int16, int32, general_cast },
            { int16, int64, general_cast },
            { int16, float32, general_cast },
            { int16, float64, general_cast },
            { int16, decimal, general_cast },
            { int32, int16, general_cast },
            { int32, int64, general_cast },
            { int32, float32, to_f32 }, // lossy
            { int32, float64, general_cast },
            { int32, decimal, general_cast },
            { int64, int16, general_cast },
            { int64, int32, general_cast },
            { int64, float32, to_f32 }, // lossy
            { int64, float64, to_f64 }, // lossy
            { int64, decimal, general_cast },

            { float32, float64, general_cast },
            { float32, decimal, general_cast },
            { float32, int16, to_i16 },
            { float32, int32, to_i32 },
            { float32, int64, to_i64 },
            { float64, decimal, general_cast },
            { float64, int16, to_i16 },
            { float64, int32, to_i32 },
            { float64, int64, to_i64 },
            { float64, float32, to_f32 }, // lossy

            { decimal, int16, dec_to_i16 },
            { decimal, int32, dec_to_i32 },
            { decimal, int64, dec_to_i64 },
            { decimal, float32, to_f32 },
            { decimal, float64, to_f64 },

            { date, timestamp, date_to_timestamp }
        }
    };
}

/// This macro helps to create neg expression.
/// It receives all the types that impl `CheckedNeg` trait.
/// * `$child`: child expression
/// * `$ret`: return expression
/// * `$input`: input type
macro_rules! gen_neg_impl {
    ($child:expr, $ret:expr, $($input:ident),*) => {
        match $child.return_type() {
            $(
                $input! {type_match_pattern} => Box::new(
                    UnaryExpression::<$input! {type_array}, $input! {type_array}, _>::new(
                        $child,
                        $ret.clone(),
                        general_neg,
                    )
                ),
            )*
            _ => {
                return Err(ErrorCode::NotImplemented(format!(
                    "Neg is not supported on {:?}",
                    $child.return_type()
                ), 112.into())
                .into());
            }
        }
    };
}

macro_rules! gen_neg {
    ($child:tt, $ret:tt) => {
        gen_neg_impl! {
            $child,
            $ret,
            int16,
            int32,
            int64,
            float32,
            float64,
            decimal
        }
    };
}

pub fn new_unary_expr(
    expr_type: ProstType,
    return_type: DataType,
    child_expr: BoxedExpression,
) -> Result<BoxedExpression> {
    use crate::expr::data_types::*;

    let expr: BoxedExpression = match (expr_type, return_type.clone(), child_expr.return_type()) {
        (ProstType::Cast, _, _) => gen_cast! { child_expr, return_type, },
        (ProstType::Not, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                conjunction::not,
            ))
        }
        (ProstType::IsTrue, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_true,
            ))
        }
        (ProstType::IsNotTrue, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_not_true,
            ))
        }
        (ProstType::IsFalse, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_false,
            ))
        }
        (ProstType::IsNotFalse, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_not_false,
            ))
        }
        (ProstType::IsNull, _, _) => Box::new(IsNullExpression::new(child_expr)),
        (ProstType::IsNotNull, _, _) => Box::new(IsNotNullExpression::new(child_expr)),
        (ProstType::Upper, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
            child_expr,
            return_type,
            upper,
        )),
        (ProstType::Lower, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
            child_expr,
            return_type,
            lower,
        )),
        (ProstType::Ascii, _, _) => Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
            child_expr,
            return_type,
            ascii,
        )),
        (ProstType::Neg, _, _) => {
            gen_neg! { child_expr, return_type }
        }
        (ProstType::PgSleep, _, DataType::Decimal) => Box::new(PgSleepExpression::new(child_expr)),

        (expr, ret, child) => {
            return Err(ErrorCode::NotImplemented(format!(
                "The expression {:?}({:?}) ->{:?} using vectorized expression framework is not supported yet.",
                expr, child, ret
            ), 112.into())
            .into());
        }
    };

    Ok(expr)
}

pub fn new_length_default(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryExpression::<Utf8Array, I64Array, _>::new(
        expr_ia1,
        return_type,
        length_default,
    ))
}

pub fn new_trim_expr(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
        expr_ia1,
        return_type,
        trim,
    ))
}

pub fn new_ltrim_expr(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
        expr_ia1,
        return_type,
        ltrim,
    ))
}

pub fn new_rtrim_expr(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
        expr_ia1,
        return_type,
        rtrim,
    ))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use itertools::Itertools;
    use num_traits::FromPrimitive;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::*;
    use risingwave_common::types::{
        Decimal, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, Scalar, ScalarImpl,
    };
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::FunctionCall;

    use super::super::*;
    use super::new_unary_expr;
    use crate::expr::test_utils::{make_expression, make_input_ref};
    use crate::vector_op::cast::{date_to_timestamp, str_parse};

    #[test]
    fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, Type::Not);
        test_unary_date::<NaiveDateTimeArray, _>(|x| date_to_timestamp(x).unwrap(), Type::Cast);
        test_str_to_int16::<I16Array, _>(|x| str_parse(x).unwrap());
    }

    #[test]
    fn test_i16_to_i32() {
        let mut input = Vec::<Option<i16>>::new();
        let mut target = Vec::<Option<i32>>::new();
        for i in 0..100i16 {
            if i % 2 == 0 {
                target.push(Some(i as i32));
                input.push(Some(i as i16));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = Column::new(
            I16Array::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int16)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    #[test]
    fn test_neg() {
        let mut input = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<i32>>::new();

        input.push(Some(1));
        input.push(Some(0));
        input.push(Some(-1));

        target.push(Some(-1));
        target.push(Some(0));
        target.push(Some(1));

        let col1 = Column::new(
            I32Array::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Neg as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int32)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_str_to_int16<A, F>(f: F)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(&str) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<String>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..1u32 {
            if i % 2 == 0 {
                let s = i.to_string();
                target.push(Some(f(&s)));
                input.push(Some(s));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = Column::new(
            Utf8Array::from_slice(&input.iter().map(|x| x.as_ref().map(|x| &**x)).collect_vec())
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let return_type = DataType {
            type_name: TypeName::Int16 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Char)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_unary_bool<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(bool) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<bool>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                input.push(Some(true));
                target.push(Some(f(true)));
            } else if i % 3 == 0 {
                input.push(Some(false));
                target.push(Some(f(false)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            BoolArray::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let expr = make_expression(kind, &[TypeName::Boolean], &[0]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_unary_date<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(NaiveDateWrapper) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<NaiveDateWrapper>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                let date = NaiveDateWrapper::new(NaiveDate::from_num_days_from_ce(i));
                input.push(Some(date));
                target.push(Some(f(date)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            NaiveDateArray::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let expr = make_expression(kind, &[TypeName::Date], &[0]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    #[test]
    fn test_same_type_cast() {
        use risingwave_common::types::DataType;
        use risingwave_pb::expr::expr_node::Type as ExprType;

        fn assert_castible<T: Into<ScalarImpl>>(t: DataType, v: T) {
            let expr = new_unary_expr(
                ExprType::Cast,
                t.clone(),
                Box::new(LiteralExpression::new(t, Some(v.into()))) as BoxedExpression,
            )
            .unwrap();
            expr.eval(&DataChunk::new_dummy(1)).unwrap();
        }
        assert_castible(DataType::Int16, 1_i16);
        assert_castible(DataType::Int32, 1_i32);
        assert_castible(DataType::Int64, 1_i64);
        assert_castible(DataType::Boolean, true);
        assert_castible(DataType::Float32, 3.0_f32);
        assert_castible(DataType::Float64, 3.0_f64);
        assert_castible(DataType::Decimal, Decimal::from_f32(3.15_f32).unwrap());
        assert_castible(DataType::Varchar, "abc".to_string());
        assert_castible(
            DataType::Date,
            NaiveDateWrapper::new_with_days(100).unwrap(),
        );
        assert_castible(
            DataType::Time,
            NaiveTimeWrapper::new_with_secs_nano(1, 1).unwrap(),
        );
        assert_castible(
            DataType::Timestamp,
            NaiveDateTimeWrapper::new_with_secs_nsecs(1, 1).unwrap(),
        );
    }
}
