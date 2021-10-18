use crate::array::{BoolArray, F32Array, F64Array, I16Array, I32Array, I64Array, UTF8Array};
use crate::expr::expr_tmpl::BinaryExpression;
use crate::expr::BoxedExpression;
use crate::types::{DataTypeKind, DataTypeRef};
use crate::vector_op::arithmetic_op::{
    float_add, float_div, float_mul, float_sub, integer_add, integer_div, integer_mul, integer_sub,
    primitive_mod,
};
use crate::vector_op::cmp::{
    primitive_eq, primitive_geq, primitive_gt, primitive_leq, primitive_lt, primitive_neq,
};
use crate::vector_op::like::like_default;
use crate::vector_op::position::position;
use risingwave_proto::expr::ExprNode_Type;
use std::marker::PhantomData;

/// The macro is responsible for specializing expressions according to the left expr and right expr.
/// Parameters:
///   `l`/`r`: the left/right child of the binary expression
///   `ret`: the return type of the binary expression
///   `int_f`/`float_f`: the scalar func for the binary
/// returns:
///   Boxed Expression
///
/// Note for scalar func:
/// For some scalar functions, the operations are different with different types, we can not put them in one generic function
///   e.g. adding for int is different from that for float
/// Thus, we should manually specialize the scalar function according to different type and pass them to the macro.
///
// TODO: Simplify using macro
macro_rules! gen_across_binary {
    ($l:expr, $r:expr, $ret: expr, $OA: ty, $int_f:ident, $float_f: ident) => {
        match (
            $l.return_type().data_type_kind(),
            $r.return_type().data_type_kind(),
        ) {
            // integer
            (DataTypeKind::Int16, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I16Array, I16Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i16, i16>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I16Array, I32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i32, i32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I16Array, I64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i64, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I16Array, F32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i16, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I16Array, F64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i16, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I32Array, I32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i32, i32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I32Array, I64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i64, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I32Array, F32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i32, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I32Array, F64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i32, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I64Array, I64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i64, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I64Array, F32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i64, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I64Array, F64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i64, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<F32Array, F32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<F32Array, F64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<F64Array, F64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I32Array, I16Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i16, i32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I64Array, I16Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i16, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I64Array, I32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i32, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<F32Array, I16Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i16, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<F32Array, I32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<F32Array, I64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i64, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<F64Array, I16Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i16, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<F64Array, I32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i32, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<F64Array, I64Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<F64Array, F32Array, $OA, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, f32, f64>,
                    _phantom: PhantomData,
                })
            }
            _ => {
                unimplemented!(
                    "The expression using vectorized expression framework is not supported yet!"
                )
            }
        }
    };
}

macro_rules! gen_arithmetic_binary {
    ($l:expr, $r:expr, $ret: expr, $int_f:ident, $float_f: ident) => {
        match (
            $l.return_type().data_type_kind(),
            $r.return_type().data_type_kind(),
        ) {
            // integer
            (DataTypeKind::Int16, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I16Array, I16Array, I16Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i16, i16>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I16Array, I32Array, I32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i32, i32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I16Array, I64Array, I64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i64, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I16Array, F32Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i16, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I16Array, F64Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i16, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I32Array, I32Array, I32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i32, i32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I32Array, I64Array, I64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i64, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I32Array, F32Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i32, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I32Array, F64Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i32, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I64Array, I64Array, I64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i64, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I64Array, F32Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i64, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I64Array, F64Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<i64, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<F32Array, F32Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, f32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<F32Array, F64Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<F64Array, F64Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, f64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I32Array, I16Array, I32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i16, i32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I64Array, I16Array, I64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i16, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I64Array, I32Array, I64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i32, i64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<F32Array, I16Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i16, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<F32Array, I32Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i32, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<F32Array, I64Array, F32Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i64, f32>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<F64Array, I16Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i16, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<F64Array, I32Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i32, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<F64Array, I64Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i64, f64>,
                    _phantom: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<F64Array, F32Array, F64Array, _> {
                    expr_ia1: $l,
                    expr_ia2: $r,
                    return_type: $ret,
                    func: $float_f::<f64, f32, f64>,
                    _phantom: PhantomData,
                })
            }
            _ => {
                unimplemented!(
                    "The expression using vectorized expression framework is not supported yet!"
                )
            }
        }
    };
}

pub fn new_binary_expr(
    expr_type: ExprNode_Type,
    ret: DataTypeRef,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        ExprNode_Type::EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_eq, primitive_eq)
        }
        ExprNode_Type::NOT_EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_neq, primitive_neq)
        }
        ExprNode_Type::LESS_THAN => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_lt, primitive_lt)
        }
        ExprNode_Type::GREATER_THAN => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_gt, primitive_gt)
        }
        ExprNode_Type::GREATER_THAN_OR_EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_geq, primitive_geq)
        }
        ExprNode_Type::LESS_THAN_OR_EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_leq, primitive_leq)
        }
        ExprNode_Type::ADD => {
            gen_arithmetic_binary! {l, r, ret, integer_add, float_add}
        }
        ExprNode_Type::SUBTRACT => {
            gen_arithmetic_binary! {l, r, ret, integer_sub, float_sub}
        }
        ExprNode_Type::MULTIPLY => {
            gen_arithmetic_binary! {l, r, ret, integer_mul, float_mul}
        }
        ExprNode_Type::DIVIDE => {
            gen_arithmetic_binary! {l, r, ret, integer_div, float_div}
        }
        ExprNode_Type::MODULUS => {
            gen_arithmetic_binary! {l, r, ret, primitive_mod, primitive_mod}
        }
        _ => {
            unimplemented!(
                "The expression using vectorized expression framework is not supported yet!"
            )
        }
    }
}

pub fn new_like_default(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryExpression::<UTF8Array, UTF8Array, BoolArray, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: like_default,
        _phantom: PhantomData,
    })
}

pub fn new_position_expr(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryExpression::<UTF8Array, UTF8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: position,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::column::Column;
    use crate::array::{Array, DataChunk};
    use crate::array::{ArrayImpl, PrimitiveArray};
    use crate::expr::build_from_proto;
    use crate::types::{Int32Type, Scalar};
    use pb_construct::make_proto;
    use protobuf::well_known_types::Any as AnyProto;
    use protobuf::RepeatedField;
    use risingwave_proto::data::DataType as DataTypeProto;
    use risingwave_proto::expr::ExprNode_Type::INPUT_REF;
    use risingwave_proto::expr::InputRefExpr;
    use risingwave_proto::expr::{ExprNode, ExprNode_Type, FunctionCall};

    #[test]
    fn test_arithmetic() {
        test_func::<I32Array, _>(|x, y| x + y, ExprNode_Type::ADD);
        test_func::<I32Array, _>(|x, y| x - y, ExprNode_Type::SUBTRACT);
        test_func::<I32Array, _>(|x, y| x * y, ExprNode_Type::MULTIPLY);
        test_func::<I32Array, _>(|x, y| x / y, ExprNode_Type::DIVIDE);
        test_func::<BoolArray, _>(|x, y| x == y, ExprNode_Type::EQUAL);
        test_func::<BoolArray, _>(|x, y| x != y, ExprNode_Type::NOT_EQUAL);
        test_func::<BoolArray, _>(|x, y| x > y, ExprNode_Type::GREATER_THAN);
        test_func::<BoolArray, _>(|x, y| x >= y, ExprNode_Type::GREATER_THAN_OR_EQUAL);
        test_func::<BoolArray, _>(|x, y| x < y, ExprNode_Type::LESS_THAN);
        test_func::<BoolArray, _>(|x, y| x <= y, ExprNode_Type::LESS_THAN_OR_EQUAL);
    }

    fn test_func<A, F>(f: F, kind: ExprNode_Type)
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

        let col1 = create_column(&lhs);
        let col2 = create_column(&rhs);
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
        let expr = make_expression(kind);
        let mut vec_excutor = build_from_proto(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn make_expression(kind: ExprNode_Type) -> ExprNode {
        let lhs = make_inputref(0);
        let rhs = make_inputref(1);
        make_proto!(ExprNode, {
          expr_type: kind,
          body: AnyProto::pack(
            &make_proto!(FunctionCall, {
              children: RepeatedField::from_slice(&[lhs, rhs])
            })
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::BOOLEAN
          })
        })
    }

    fn make_inputref(idx: i32) -> ExprNode {
        make_proto!(ExprNode, {
          expr_type: INPUT_REF,
          body: AnyProto::pack(
            &make_proto!(InputRefExpr, {column_idx: idx})
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::INT32
          })
        })
    }

    fn create_column(vec: &[Option<i32>]) -> Column {
        let array = PrimitiveArray::from_slice(vec)
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let data_type = Arc::new(Int32Type::new(false));
        Column::new(array, data_type)
    }
}
