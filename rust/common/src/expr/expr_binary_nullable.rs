/// For expression that only accept two nullable arguments as input.
use std::marker::PhantomData;

use super::BoxedExpression;
use crate::array::{BoolArray, DecimalArray, F32Array, F64Array, I16Array, I32Array, I64Array};
use crate::error::Result;
use crate::expr::template::BinaryNullableExpression;
use crate::types::DataTypeKind;
use crate::types::DataTypeRef;
use crate::vector_op::conjunction::{and, or};
use risingwave_pb::expr::expr_node::Type;

// TODO: consider implement it using generic function.
macro_rules! gen_stream_null_by_row_count_expr {
    ($l:expr, $r:expr, $ret: expr, $OA:ty) => {
        Box::new(BinaryNullableExpression::<I64Array, $OA, $OA, _> {
            expr_ia1: $l,
            expr_ia2: $r,
            return_type: $ret,
            func: stream_null_by_row_count,
            _phantom: PhantomData,
        })
    };
}

//  stream_null_by_row_count is mainly used for a special case of the conditional aggregation.
//  For example, SELECT stream_null_by_row_count(row_count, x) FROM (SELECT count(*) AS row_count,
// avg(x) AS x FROM t1)  is equivalent to the SELECT avg(case when row_count > 0 then x else NULL)
// FROM t1;
fn stream_null_by_row_count<T1>(l: Option<i64>, r: Option<T1>) -> Result<Option<T1>> {
    Ok(l.filter(|l| *l > 0).and(r))
}

pub fn new_nullable_binary_expr(
    expr_type: Type,
    ret: DataTypeRef,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        Type::StreamNullByRowCount => match l.return_type().data_type_kind() {
            DataTypeKind::Int64 => match r.return_type().data_type_kind() {
                DataTypeKind::Boolean => gen_stream_null_by_row_count_expr!(l, r, ret, BoolArray),
                DataTypeKind::Int16 => gen_stream_null_by_row_count_expr!(l, r, ret, I16Array),
                DataTypeKind::Int32 => gen_stream_null_by_row_count_expr!(l, r, ret, I32Array),
                DataTypeKind::Int64 => gen_stream_null_by_row_count_expr!(l, r, ret, I64Array),
                DataTypeKind::Float32 => gen_stream_null_by_row_count_expr!(l, r, ret, F32Array),
                DataTypeKind::Float64 => gen_stream_null_by_row_count_expr!(l, r, ret, F64Array),
                DataTypeKind::Decimal => {
                    gen_stream_null_by_row_count_expr!(l, r, ret, DecimalArray)
                }
                DataTypeKind::Date => gen_stream_null_by_row_count_expr!(l, r, ret, DecimalArray),
                _ => {
                    unimplemented!(
                        "The output type isn't supported by stream_null_by_row_count function."
                    )
                }
            },
            tp => {
                unimplemented!(
                    "The first argument of StreamNullByRowCount must be Int64 but not {:?}",
                    tp
                )
            }
        },
        Type::And => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _> {
                expr_ia1: l,
                expr_ia2: r,
                return_type: ret,
                func: and,
                _phantom: PhantomData,
            },
        ),
        Type::Or => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _> {
                expr_ia1: l,
                expr_ia2: r,
                return_type: ret,
                func: or,
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

#[cfg(test)]
mod tests {
    use super::stream_null_by_row_count;

    #[test]
    fn test_stream_if_not_null() {
        let cases = [
            (Some(1), Some(2), Some(2)),
            (Some(0), Some(2), None),
            (Some(3), None, None),
            (None, Some(3), None),
        ];
        for (arg1, arg2, expected) in cases {
            let output =
                stream_null_by_row_count(arg1, arg2).expect("No error in stream_null_by_row_count");
            assert_eq!(output, expected);
        }
    }
}
