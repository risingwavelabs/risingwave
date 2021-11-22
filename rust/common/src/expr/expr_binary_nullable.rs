/// For expression that only accept two nullable arguments as input.
use std::marker::PhantomData;

use super::BoxedExpression;
use crate::array::{BoolArray, DecimalArray, F32Array, F64Array, I16Array, I32Array, I64Array};
use crate::error::Result;
use crate::expr::template::BinaryNullableExpression;
use crate::types::DataTypeKind;
use crate::types::DataTypeRef;
use risingwave_pb::expr::expr_node::Type;

#[allow(unused_macros)]
macro_rules! gen_across_binary {
  ($l:expr, $r:expr, $ret: expr, $OA: ty, $f:ident) => {
    match (
      $l.return_type().data_type_kind(),
      $r.return_type().data_type_kind(),
    ) {
      // integer
      (DataTypeKind::Int64, DataTypeKind::Int64) => {
        Box::new(BinaryNullableExpression::<I64Array, $OA, $OA, _> {
          expr_ia1: $l,
          expr_ia2: $r,
          return_type: $ret,
          func: $f,
          _phantom: PhantomData,
        })
      }
      tp => {
        unimplemented!(
          "The expression {:?} using vectorized expression framework is not supported yet!",
          tp
        )
      }
    }
  };
}

//  stream_null_by_row_count is mainly used for a spcial case of the conditional aggregation.
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
        Type::StreamNullByRowCount => match r.return_type().data_type_kind() {
            DataTypeKind::Boolean => {
                gen_across_binary!(l, r, ret, BoolArray, stream_null_by_row_count)
            }
            DataTypeKind::Int16 => {
                gen_across_binary!(l, r, ret, I16Array, stream_null_by_row_count)
            }
            DataTypeKind::Int32 => {
                gen_across_binary!(l, r, ret, I32Array, stream_null_by_row_count)
            }
            DataTypeKind::Int64 => {
                gen_across_binary!(l, r, ret, I64Array, stream_null_by_row_count)
            }
            DataTypeKind::Float32 => {
                gen_across_binary!(l, r, ret, F32Array, stream_null_by_row_count)
            }
            DataTypeKind::Float64 => {
                gen_across_binary!(l, r, ret, F64Array, stream_null_by_row_count)
            }
            DataTypeKind::Decimal => {
                gen_across_binary!(l, r, ret, DecimalArray, stream_null_by_row_count)
            }
            DataTypeKind::Date => {
                gen_across_binary!(l, r, ret, DecimalArray, stream_null_by_row_count)
            }
            _ => unimplemented!(
                "The output type isn't supported by stream_null_by_row_count function."
            ),
        },
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
