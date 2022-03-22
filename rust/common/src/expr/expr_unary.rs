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
//
//! For expression that only accept one value as input (e.g. CAST)

use risingwave_pb::expr::expr_node::Type as ProstType;

use super::template::{UnaryBytesExpression, UnaryExpression};
use crate::array::*;
use crate::expr::expr_is_null::{IsNotNullExpression, IsNullExpression};
use crate::expr::pg_sleep::PgSleepExpression;
use crate::expr::template::UnaryNullableExpression;
use crate::expr::BoxedExpression;
use crate::types::*;
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
    ([$child:expr, $ret:expr], $( { $input:tt, $cast:tt, $func:expr } ),*) => {
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
                unimplemented!("CAST({:?} AS {:?}) not supported yet!", $child.return_type(), $ret)
            }
        }
    };
}

macro_rules! gen_cast {
    ($($x:tt, )* ) => {
        gen_cast_impl! {
        [$($x),*],
        { varchar, date, str_to_date},
        { varchar, timestamp, str_to_timestamp},
        { varchar, timestampz, str_to_timestampz},
        { varchar, int16, str_to_i16},
        { varchar, int32, str_to_i32},
        { varchar, int64, str_to_i64},
        { varchar, float32, str_to_real},
        { varchar, float64, str_to_double},
        { varchar, decimal, str_to_decimal},
        { varchar, boolean, str_to_bool},
        { boolean, varchar, bool_to_str},
        // TODO: decide whether nullability-cast should be allowed (#2350)
        { boolean, boolean, |x| Ok(x)},
        { int16, int32, general_cast },
        { int16, int64, general_cast },
        { int16, float32, general_cast },
        { int16, float64, general_cast },
        { int16, decimal, general_cast },
        { int32, int16, general_cast },
        { int32, int64, general_cast },
        { int32, float64, general_cast },
        { int32, decimal, general_cast },
        { int64, int16, general_cast },
        { int64, int32, general_cast },
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
        { decimal, decimal, dec_to_dec },
        { decimal, int16, deci_to_i16 },
        { decimal, int32, deci_to_i32 },
        { decimal, int64, deci_to_i64 },
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
    ($child:expr, $ret:expr, $($input:tt),*) => {
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
                unimplemented!("Neg is not supported on {:?}", $child.return_type())
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
) -> BoxedExpression {
    use crate::expr::data_types::*;

    match (expr_type, return_type.clone(), child_expr.return_type()) {
        // FIXME: We can not unify char and varchar because they are different in PG while share the
        // same logical type (String type) in our system (#2414).
        (ProstType::Cast, DataType::Date, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, NaiveDateArray, _>::new(
                child_expr,
                return_type,
                str_to_date,
            ))
        }
        (ProstType::Cast, DataType::Time, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, NaiveTimeArray, _>::new(
                child_expr,
                return_type,
                str_to_time,
            ))
        }
        (ProstType::Cast, DataType::Timestamp, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, NaiveDateTimeArray, _>::new(
                child_expr,
                return_type,
                str_to_timestamp,
            ))
        }
        (ProstType::Cast, DataType::Timestampz, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _>::new(
                child_expr,
                return_type,
                str_to_timestampz,
            ))
        }
        (ProstType::Cast, DataType::Boolean, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, BoolArray, _>::new(
                child_expr,
                return_type,
                str_to_bool,
            ))
        }
        (ProstType::Cast, DataType::Decimal, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, DecimalArray, _>::new(
                child_expr,
                return_type,
                str_to_decimal,
            ))
        }
        (ProstType::Cast, DataType::Float32, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, F32Array, _>::new(
                child_expr,
                return_type,
                str_to_real,
            ))
        }
        (ProstType::Cast, DataType::Float64, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, F64Array, _>::new(
                child_expr,
                return_type,
                str_to_double,
            ))
        }
        (ProstType::Cast, DataType::Int16, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I16Array, _>::new(
                child_expr,
                return_type,
                str_to_i16,
            ))
        }
        (ProstType::Cast, DataType::Int32, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
                child_expr,
                return_type,
                str_to_i32,
            ))
        }
        (ProstType::Cast, DataType::Int64, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _>::new(
                child_expr,
                return_type,
                str_to_i64,
            ))
        }
        (ProstType::Cast, DataType::Char, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, Utf8Array, _>::new(
                child_expr,
                return_type,
                str_to_str,
            ))
        }
        (ProstType::Cast, DataType::Varchar, DataType::Char) => {
            Box::new(UnaryExpression::<Utf8Array, Utf8Array, _>::new(
                child_expr,
                return_type,
                str_to_str,
            ))
        }
        (ProstType::Cast, _, _) => gen_cast! {child_expr, return_type, },
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
            unimplemented!("The expression {:?}({:?}) ->{:?} using vectorized expression framework is not supported yet!", expr, child, ret)
        }
    }
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
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::FunctionCall;

    use super::super::*;
    use crate::array::column::Column;
    use crate::array::*;
    use crate::expr::test_utils::{make_expression, make_input_ref};
    use crate::types::{NaiveDateWrapper, Scalar};
    use crate::vector_op::cast::{date_to_timestamp, str_to_i16};

    #[test]
    fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, Type::Not);
        test_unary_date::<NaiveDateTimeArray, _>(|x| date_to_timestamp(x).unwrap(), Type::Cast);
        test_str_to_int16::<I16Array, _>(|x| str_to_i16(x).unwrap());
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
}
