// Copyright 2023 RisingWave Labs
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

//! For expression that only accept two nullable arguments as input.

use std::sync::Arc;

use risingwave_common::array::serial_array::SerialArray;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_pb::expr::expr_node::Type;

use super::{BoxedExpression, Expression};
use crate::expr::template::BinaryNullableExpression;
use crate::expr::template_fast;
use crate::vector_op::array_access::array_access;
use crate::vector_op::cmp::{
    general_is_distinct_from, general_is_not_distinct_from, general_ne, str_is_distinct_from,
    str_is_not_distinct_from,
};
use crate::vector_op::conjunction::{and, or};
use crate::vector_op::format_type::format_type;
use crate::{for_all_cmp_variants, ExprError, Result};

macro_rules! gen_is_distinct_from_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $cast:ident, $func:ident} ),* $(,)?) => {
        match ($l.return_type(), $r.return_type()) {
            $(
                ($i1! { type_match_pattern }, $i2! { type_match_pattern }) => {
                    template_fast::IsDistinctFromExpression::new(
                        $l,
                        $r,
                        general_ne::<
                            <$i1! { type_array } as Array>::OwnedItem,
                            <$i2! { type_array } as Array>::OwnedItem,
                            <$cast! { type_array } as Array>::OwnedItem
                        >,
                        $func,
                    ).boxed()
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
        let left = self.expr_ia1.eval_checked(input)?;
        let left = left.as_bool();

        let res_vis: Vis = match self.expr_type {
            // For `Or` operator, if res of left part is not null and is true, we do not want to
            // calculate right part because the result must be true.
            Type::Or => (!left.to_bitmap()).into(),
            // For `And` operator, If res of left part is not null and is false, we do not want
            // to calculate right part because the result must be false.
            Type::And => (left.data() | !left.null_bitmap()).into(),
            _ => unimplemented!(),
        };
        let new_vis = input.vis() & res_vis;
        let mut input1 = input.clone();
        input1.set_vis(new_vis);

        let right = self.expr_ia2.eval_checked(&input1)?;
        let right = right.as_bool();
        assert_eq!(left.len(), right.len());

        let mut bitmap = match input.visibility() {
            Some(vis) => vis.clone(),
            None => Bitmap::ones(input.capacity()),
        };
        bitmap &= left.null_bitmap();
        bitmap &= right.null_bitmap();

        let c = match self.expr_type {
            Type::Or => {
                let data = left.to_bitmap() | right.to_bitmap();
                bitmap |= &data; // is_true || is_true
                BoolArray::new(data, bitmap)
            }
            Type::And => {
                let data = left.to_bitmap() & right.to_bitmap();
                bitmap |= !left.data() & left.null_bitmap(); // is_false
                bitmap |= !right.data() & right.null_bitmap(); // is_false
                BoolArray::new(data, bitmap)
            }
            _ => unimplemented!(),
        };
        Ok(Arc::new(c.into()))
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let ret_ia1 = self.expr_ia1.eval_row(input)?.map(|x| x.into_bool());
        match self.expr_type {
            Type::Or if ret_ia1 == Some(true) => return Ok(Some(true.to_scalar_value())),
            Type::And if ret_ia1 == Some(false) => return Ok(Some(false.to_scalar_value())),
            _ => {}
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
        Type::FormatType => new_format_type_expr(l, r, ret),
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
        DataType::Serial => array_access_expression!(SerialArray),
        DataType::Float32 => array_access_expression!(F32Array),
        DataType::Float64 => array_access_expression!(F64Array),
        DataType::Decimal => array_access_expression!(DecimalArray),
        DataType::Date => array_access_expression!(NaiveDateArray),
        DataType::Varchar => array_access_expression!(Utf8Array),
        DataType::Bytea => array_access_expression!(BytesArray),
        DataType::Time => array_access_expression!(NaiveTimeArray),
        DataType::Timestamp => array_access_expression!(NaiveDateTimeArray),
        DataType::Timestamptz => array_access_expression!(PrimitiveArray::<i64>),
        DataType::Interval => array_access_expression!(IntervalArray),
        DataType::Jsonb => array_access_expression!(JsonbArray),
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
        (DataType::Boolean, DataType::Boolean) => template_fast::BooleanBinaryExpression::new(
            l,
            r,
            |l, r| {
                let data = ((l.data() ^ r.data()) & (l.null_bitmap() & r.null_bitmap()))
                    | (l.null_bitmap() ^ r.null_bitmap());
                BoolArray::new(data, Bitmap::ones(l.len()))
            },
            |l, r| Some(general_is_distinct_from::<bool, bool, bool>(l, r)),
        )
        .boxed(),
        (DataType::Varchar, DataType::Varchar) => Box::new(BinaryNullableExpression::<
            Utf8Array,
            Utf8Array,
            BoolArray,
            _,
        >::new(
            l, r, ret, str_is_distinct_from
        )),
        _ => {
            for_all_cmp_variants! { gen_is_distinct_from_impl, l, r, ret, false }
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
        (DataType::Boolean, DataType::Boolean) => template_fast::BooleanBinaryExpression::new(
            l,
            r,
            |l, r| {
                let data = !(((l.data() ^ r.data()) & (l.null_bitmap() & r.null_bitmap()))
                    | (l.null_bitmap() ^ r.null_bitmap()));
                BoolArray::new(data, Bitmap::ones(l.len()))
            },
            |l, r| Some(general_is_not_distinct_from::<bool, bool, bool>(l, r)),
        )
        .boxed(),
        (DataType::Varchar, DataType::Varchar) => Box::new(BinaryNullableExpression::<
            Utf8Array,
            Utf8Array,
            BoolArray,
            _,
        >::new(
            l, r, ret, str_is_not_distinct_from
        )),
        _ => {
            for_all_cmp_variants! { gen_is_distinct_from_impl, l, r, ret, true }
        }
    };
    Ok(expr)
}

pub fn new_format_type_expr(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    Box::new(
        BinaryNullableExpression::<I32Array, I32Array, Utf8Array, _>::new(
            expr_ia1,
            expr_ia2,
            return_type,
            format_type,
        ),
    )
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
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
            let row = OwnedRow::new(vec![
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
            let row = OwnedRow::new(vec![
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
            let row = OwnedRow::new(vec![
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
            let row = OwnedRow::new(vec![
                lhs[i].map(|x| x.to_scalar_value()),
                rhs[i].map(|x| x.to_scalar_value()),
            ]);
            let res = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|x| x.to_scalar_value());
            assert_eq!(res, expected);
        }
    }

    #[test]
    fn test_format_type() {
        let l = vec![Some(16), Some(21), Some(9527), None];
        let r = vec![Some(0), None, Some(0), Some(0)];
        let target: Vec<Option<String>> = vec![
            Some("boolean".into()),
            Some("smallint".into()),
            Some("???".into()),
            None,
        ];
        let expr = make_expression(
            Type::FormatType,
            &[TypeName::Int32, TypeName::Int32],
            &[0, 1],
        );
        let vec_executor = build_from_prost(&expr).unwrap();

        for i in 0..l.len() {
            let row = OwnedRow::new(vec![
                l[i].map(|x| x.to_scalar_value()),
                r[i].map(|x| x.to_scalar_value()),
            ]);
            let res = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().map(|x| x.into());
            assert_eq!(res, expected);
        }
    }
}
