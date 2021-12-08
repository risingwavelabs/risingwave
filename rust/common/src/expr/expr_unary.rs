/// For expression that only accept one value as input (e.g. CAST)
use super::template::{UnaryBytesExpression, UnaryExpression};
use crate::array::{BoolArray, I32Array, I64Array, Utf8Array};
use crate::expr::pg_sleep::PgSleepExpression;
use crate::expr::template::UnaryNullableExpression;
use crate::expr::BoxedExpression;
use crate::types::{DataTypeKind, DataTypeRef};
use crate::vector_op::length::length_default;
use crate::vector_op::ltrim::ltrim;
use crate::vector_op::rtrim::rtrim;
use crate::vector_op::trim::trim;
use crate::vector_op::upper::upper;
use crate::vector_op::{cast, conjunction};
use risingwave_pb::expr::expr_node::Type as ProstType;
use std::marker::PhantomData;

pub fn new_unary_expr(
    expr_type: ProstType,
    return_type: DataTypeRef,
    child_expr: BoxedExpression,
) -> BoxedExpression {
    match (
        expr_type,
        return_type.data_type_kind(),
        child_expr.return_type().data_type_kind(),
    ) {
        (ProstType::Cast, DataTypeKind::Date, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I32Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_date,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Time, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_time,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Timestamp, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_timestamp,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Timestampz, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_timestampz,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Boolean, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, BoolArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_bool,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Boolean, DataTypeKind::Varchar) => {
            Box::new(UnaryExpression::<Utf8Array, BoolArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_bool,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Timestamp, DataTypeKind::Date) => {
            Box::new(UnaryExpression::<I32Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::date_to_timestamp,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Int32, DataTypeKind::Int64) => {
            Box::new(UnaryExpression::<I64Array, I32Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: |x: i64| Ok(x as i32),
                _phantom: PhantomData,
            })
        }
        (ProstType::Not, _, _) => Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _> {
            expr_ia1: child_expr,
            return_type,
            func: conjunction::not,
            _phantom: PhantomData,
        }),
        (ProstType::Upper, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _> {
            expr_ia1: child_expr,
            return_type,
            func: upper,
            _phantom: PhantomData,
        }),
        (ProstType::PgSleep, _, DataTypeKind::Decimal) => {
            Box::new(PgSleepExpression::new(child_expr))
        }
        (expr, ret, child) => {
            unimplemented!("The expression {:?}({:?}) ->{:?} using vectorized expression framework is not supported yet!", expr, child, ret)
        }
    }
}

pub fn new_length_default(expr_ia1: BoxedExpression, return_type: DataTypeRef) -> BoxedExpression {
    Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
        expr_ia1,
        return_type,
        func: length_default,
        _phantom: PhantomData,
    })
}

pub fn new_trim_expr(expr_ia1: BoxedExpression, return_type: DataTypeRef) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _> {
        expr_ia1,
        return_type,
        func: trim,
        _phantom: PhantomData,
    })
}

pub fn new_ltrim_expr(expr_ia1: BoxedExpression, return_type: DataTypeRef) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _> {
        expr_ia1,
        return_type,
        func: ltrim,
        _phantom: PhantomData,
    })
}

pub fn new_rtrim_expr(expr_ia1: BoxedExpression, return_type: DataTypeRef) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _> {
        expr_ia1,
        return_type,
        func: rtrim,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::array::column::Column;
    use crate::array::*;
    use crate::expr::test_utils::make_expression;
    use crate::types::{BoolType, DateType, Scalar};
    use crate::vector_op::cast::date_to_timestamp;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::expr_node::Type as ProstExprType;

    #[test]
    fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, ProstExprType::Not);
        test_unary_date::<I64Array, _>(|x| date_to_timestamp(x).unwrap(), ProstExprType::Cast);
    }

    fn test_unary_bool<A, F>(f: F, kind: ProstExprType)
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
            BoolType::create(true),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let expr = make_expression(kind, &[TypeName::Boolean], &[0]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_unary_date<A, F>(f: F, kind: ProstExprType)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(i32) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                input.push(Some(i));
                target.push(Some(f(i)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            I32Array::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
            DateType::create(true),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let expr = make_expression(kind, &[TypeName::Date], &[0]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }
}
