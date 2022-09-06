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

//! For expression that only accept two nullable arguments as input.

use risingwave_common::array::*;
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;

use super::BoxedExpression;
use crate::expr::template::BinaryNullableExpression;
use crate::for_all_cmp_variants;
use crate::vector_op::array_access::array_access;
use crate::vector_op::cmp::{
    general_is_distinct_from, general_is_not_distinct_from, str_is_distinct_from,
    str_is_not_distinct_from,
};
use crate::vector_op::conjunction::{and, or};

macro_rules! gen_nullable_cmp_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $cast:ident, $func:ident} ),* $(,)?) => {
        match ($l.return_type(), $r.return_type()) {
            $(
                ($i1! { type_match_pattern }, $i2! { type_match_pattern }) => {
                    Box::new(
                        BinaryNullableExpression::<
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

pub fn new_nullable_binary_expr(
    expr_type: Type,
    ret: DataType,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        Type::ArrayAccess => build_array_access_expr(ret, l, r),
        Type::And => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, and),
        ),
        Type::Or => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, or),
        ),
        Type::IsDistinctFrom => new_distinct_from_expr(l, r, ret),
        Type::IsNotDistinctFrom => new_not_distinct_from_expr(l, r, ret),
        tp => {
            unimplemented!(
                "The expression {:?} using vectorized expression framework is not supported yet!",
                tp
            )
        }
    }
}

fn build_array_access_expr(
    ret: DataType,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    macro_rules! array_access_expression {
        ($array:ty) => {
            Box::new(
                BinaryNullableExpression::<ListArray, I32Array, $array, _>::new(
                    l,
                    r,
                    ret,
                    array_access,
                ),
            )
        };
    }

    match ret {
        DataType::Boolean => array_access_expression!(BoolArray),
        DataType::Int16 => array_access_expression!(I16Array),
        DataType::Int32 => array_access_expression!(I32Array),
        DataType::Int64 => array_access_expression!(I64Array),
        DataType::Float32 => array_access_expression!(F32Array),
        DataType::Float64 => array_access_expression!(F64Array),
        DataType::Decimal => array_access_expression!(DecimalArray),
        DataType::Date => array_access_expression!(NaiveDateArray),
        DataType::Varchar => array_access_expression!(Utf8Array),
        DataType::Time => array_access_expression!(NaiveTimeArray),
        DataType::Timestamp => array_access_expression!(NaiveDateTimeArray),
        DataType::Timestampz => array_access_expression!(PrimitiveArray::<i64>),
        DataType::Interval => array_access_expression!(IntervalArray),
        DataType::Struct { .. } => array_access_expression!(StructArray),
        DataType::List { .. } => array_access_expression!(ListArray),
    }
}

pub fn new_distinct_from_expr(
    l: BoxedExpression,
    r: BoxedExpression,
    ret: DataType,
) -> BoxedExpression {
    use crate::expr::data_types::*;

    match (l.return_type(), r.return_type()) {
        (DataType::Varchar, DataType::Varchar) => Box::new(BinaryNullableExpression::<
            Utf8Array,
            Utf8Array,
            BoolArray,
            _,
        >::new(
            l, r, ret, str_is_distinct_from
        )),
        _ => {
            for_all_cmp_variants! {gen_nullable_cmp_impl, l, r, ret, general_is_distinct_from}
        }
    }
}

pub fn new_not_distinct_from_expr(
    l: BoxedExpression,
    r: BoxedExpression,
    ret: DataType,
) -> BoxedExpression {
    use crate::expr::data_types::*;

    match (l.return_type(), r.return_type()) {
        (DataType::Varchar, DataType::Varchar) => Box::new(BinaryNullableExpression::<
            Utf8Array,
            Utf8Array,
            BoolArray,
            _,
        >::new(
            l, r, ret, str_is_not_distinct_from
        )),
        _ => {
            for_all_cmp_variants! {gen_nullable_cmp_impl, l, r, ret, general_is_not_distinct_from}
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::Row;
    use risingwave_common::types::Scalar;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::expr_node::Type;

    use crate::expr::build_from_prost;
    use crate::expr::test_utils::make_expression;

    #[test]
    fn test_and() {
        let lhs = vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ];
        let rhs = vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ];
        let target = vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            None,
        ];

        let expr = make_expression(Type::And, &[TypeName::Boolean, TypeName::Boolean], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[test]
    fn test_or() {
        let lhs = vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ];
        let rhs = vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ];
        let target = vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            None,
        ];

        let expr = make_expression(Type::Or, &[TypeName::Boolean, TypeName::Boolean], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[test]
    fn test_is_distinct_from() {
        let lhs = vec![None, None, Some(1), Some(2), Some(3)];
        let rhs = vec![None, Some(1), None, Some(2), Some(4)];
        let target = vec![Some(false), Some(true), Some(true), Some(false), Some(true)];

        let expr = make_expression(
            Type::IsDistinctFrom,
            &[TypeName::Int32, TypeName::Int32],
            &[0, 1],
        );
        let vec_executor = build_from_prost(&expr).unwrap();

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[test]
    fn test_is_not_distinct_from() {
        let lhs = vec![None, None, Some(1), Some(2), Some(3)];
        let rhs = vec![None, Some(1), None, Some(2), Some(4)];
        let target = vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ];

        let expr = make_expression(
            Type::IsNotDistinctFrom,
            &[TypeName::Int32, TypeName::Int32],
            &[0, 1],
        );
        let vec_executor = build_from_prost(&expr).unwrap();

        for i in 0..lhs.len() {
            let row = Row::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }
}
