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

use std::sync::Arc;

use itertools::{multizip, Itertools};
use risingwave_common::array::*;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_pb::expr::expr_node::Type;

use super::{BoxedExpression, Expression};
use crate::expr::template::BinaryNullableExpression;
use crate::vector_op::array_access::array_access;
use crate::vector_op::cmp::{
    general_is_distinct_from, general_is_not_distinct_from, str_is_distinct_from,
    str_is_not_distinct_from,
};
use crate::vector_op::conjunction::{and, or};
use crate::{for_all_cmp_variants, ExprError, Result};

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
                return Err(ExprError::UnsupportedFunction(format!(
                    "{:?} cmp {:?}",
                    $l.return_type(), $r.return_type()
                )));
            }
        }
    };
}

pub struct BinaryShortCircuitExpression {
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    expr_type: Type,
}

impl std::fmt::Debug for BinaryShortCircuitExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryShortCircuitExpression")
            .field("expr_ia1", &self.expr_ia1)
            .field("expr_ia2", &self.expr_ia2)
            .field("expr_type", &self.expr_type)
            .finish()
    }
}

impl Expression for BinaryShortCircuitExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let init_vis = input.vis();
        let mut input = input.clone();
        let len = input.capacity();
        let mut children_array = Vec::with_capacity(2);
        for child in [&self.expr_ia1, &self.expr_ia2] {
            let res = child.eval_checked(&input)?;
            let res_bool = res.as_bool();
            let orig_vis = input.vis();
            let res_vis: Vis = match self.expr_type {
                // For `Or` operator, if res of left part is not null and is true, we do not want to
                // calculate right part because the result must be true.
                Type::Or => (!(res_bool.to_bitmap())).into(),
                // For `And` operator, If res of left part is not null and is false, we do not want
                // to calculate right part because the result must be false.
                Type::And => (res_bool.to_bitmap() | !res_bool.null_bitmap()).into(),
                _ => unimplemented!(),
            };
            let new_vis = orig_vis & res_vis;
            input.set_vis(new_vis);
            children_array.push(res);
        }
        let mut builder =
            <BoolArray as Array>::Builder::with_meta(len, (&self.return_type()).into());
        match self.expr_type {
            Type::Or => {
                for (((v_ia1, v_ia2), init_visible), final_visible) in multizip((
                    children_array[0].as_bool().iter(),
                    children_array[1].as_bool().iter(),
                ))
                .zip_eq(init_vis.iter())
                .zip_eq(input.vis().iter())
                {
                    if init_visible {
                        builder.append(if final_visible {
                            or(v_ia1, v_ia2)?
                        } else {
                            Some(true)
                        });
                    } else {
                        builder.append_null()
                    }
                }
            }
            Type::And => {
                for (((v_ia1, v_ia2), init_visible), final_visible) in multizip((
                    children_array[0].as_bool().iter(),
                    children_array[1].as_bool().iter(),
                ))
                .zip_eq(init_vis.iter())
                .zip_eq(input.vis().iter())
                {
                    if init_visible {
                        builder.append(if final_visible {
                            and(v_ia1, v_ia2)?
                        } else {
                            Some(false)
                        });
                    } else {
                        builder.append_null()
                    }
                }
            }
            _ => unimplemented!(),
        }
        Ok(Arc::new(builder.finish().into()))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let ret_ia1 = self.expr_ia1.eval_row(input)?.map(|x| x.into_bool());
        match self.expr_type {
            Type::Or => {
                if ret_ia1 == Some(true) {
                    return Ok(Some(true.to_scalar_value()));
                }
            }
            Type::And => {
                if ret_ia1 == Some(false) {
                    return Ok(Some(false.to_scalar_value()));
                }
            }
            _ => unimplemented!(),
        }
        let ret_ia2 = self.expr_ia2.eval_row(input)?.map(|x| x.into_bool());
        match self.expr_type {
            Type::Or => Ok(or(ret_ia1, ret_ia2)?.map(|x| x.to_scalar_value())),
            Type::And => Ok(and(ret_ia1, ret_ia2)?.map(|x| x.to_scalar_value())),
            _ => unimplemented!(),
        }
    }
}

impl BinaryShortCircuitExpression {
    pub fn new(expr_ia1: BoxedExpression, expr_ia2: BoxedExpression, expr_type: Type) -> Self {
        Self {
            expr_ia1,
            expr_ia2,
            expr_type,
        }
    }
}

pub fn new_nullable_binary_expr(
    expr_type: Type,
    ret: DataType,
    l: BoxedExpression,
    r: BoxedExpression,
) -> Result<BoxedExpression> {
    let expr = match expr_type {
        Type::ArrayAccess => build_array_access_expr(ret, l, r),
        Type::And => Box::new(BinaryShortCircuitExpression::new(l, r, expr_type)),
        Type::Or => Box::new(BinaryShortCircuitExpression::new(l, r, expr_type)),
        Type::IsDistinctFrom => new_distinct_from_expr(l, r, ret)?,
        Type::IsNotDistinctFrom => new_not_distinct_from_expr(l, r, ret)?,
        tp => {
            return Err(ExprError::UnsupportedFunction(format!(
                "{:?}({:?}, {:?})",
                tp,
                l.return_type(),
                r.return_type(),
            )));
        }
    };
    Ok(expr)
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
) -> Result<BoxedExpression> {
    use crate::expr::data_types::*;

    let expr: BoxedExpression = match (l.return_type(), r.return_type()) {
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
    };
    Ok(expr)
}

pub fn new_not_distinct_from_expr(
    l: BoxedExpression,
    r: BoxedExpression,
    ret: DataType,
) -> Result<BoxedExpression> {
    use crate::expr::data_types::*;

    let expr: BoxedExpression = match (l.return_type(), r.return_type()) {
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
    };
    Ok(expr)
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
