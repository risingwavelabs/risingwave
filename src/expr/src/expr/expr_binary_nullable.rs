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

use risingwave_common::array::{Array, BoolArray, Utf8Array};
use risingwave_common::types::*;
use risingwave_pb::expr::expr_node::Type;

use super::BoxedExpression;
use crate::expr::data_types::*;
use crate::expr::template::BinaryNullableExpression;
use crate::vector_op::cmp::{general_is_distinct_from, str_is_distinct_from};
use crate::vector_op::conjunction::{and, or};

macro_rules! for_all_types {
    ($macro:ident, $l:expr, $r:expr, $ret:expr, $general_f:ident) => {
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
            { decimal, int16, decimal, $general_f },
            { decimal, int32, decimal, $general_f },
            { decimal, int64, decimal, $general_f },
            { decimal, float32, float64, $general_f },
            { decimal, float64, float64, $general_f },
            { int16, decimal, decimal, $general_f },
            { int32, decimal, decimal, $general_f },
            { int64, decimal, decimal, $general_f },
            { decimal, decimal, decimal, $general_f },
            { float32, decimal, float64, $general_f },
            { float64, decimal, float64, $general_f },
            { timestamp, timestamp, timestamp, $general_f },
            { date, date, date, $general_f },
            { boolean, boolean, boolean, $general_f },
            { timestamp, date, timestamp, $general_f },
            { date, timestamp, timestamp, $general_f }
        }
    };
}

macro_rules! gen_nullable_cmp_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $cast:ident, $func:ident} ),*) => {
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
        Type::And => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, and),
        ),
        Type::Or => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, or),
        ),
        Type::IsDistinctFrom => new_distinct_from_expr(l, r, ret),
        tp => {
            unimplemented!(
                "The expression {:?} using vectorized expression framework is not supported yet!",
                tp
            )
        }
    }
}

pub fn new_distinct_from_expr(
    l: BoxedExpression,
    r: BoxedExpression,
    ret: DataType,
) -> BoxedExpression {
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
            for_all_types! {gen_nullable_cmp_impl, l, r, ret, general_is_distinct_from}
        }
    }
}
