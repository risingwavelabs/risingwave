use crate::array2::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, BoolArray, DataChunk, F32Array, F64Array, I16Array,
    I32Array, I64Array,
};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{DataType, DataTypeKind, DataTypeRef, Scalar};
use crate::vector_op::cmp::{
    primitive_eq, primitive_geq, primitive_gt, primitive_leq, primitive_lt, primitive_neq,
};
use risingwave_proto::expr::ExprNode_ExprNodeType;
use std::marker::PhantomData;
use std::sync::Arc;
struct BinaryExpression<
    IA1: Array,
    IA2: Array,
    OA: Array,
    F: for<'a> Fn(IA1::RefItem<'a>, IA2::RefItem<'a>) -> Result<OA::OwnedItem>,
> {
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    return_type: DataTypeRef,
    func: F,
    data1: PhantomData<(IA1, IA2, OA)>,
}

impl<
        IA1: Array,
        IA2: Array,
        OA: Array,
        F: for<'a> Fn(IA1::RefItem<'a>, IA2::RefItem<'a>) -> Result<OA::OwnedItem>
            + Sized
            + Sync
            + Send,
    > Expression for BinaryExpression<IA1, IA2, OA, F>
where
    for<'a> &'a IA1: std::convert::From<&'a ArrayImpl>,
    for<'a> &'a IA2: std::convert::From<&'a ArrayImpl>,
    for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
{
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        let left_ret = self.left_expr.eval(data_chunk)?;
        let right_ret = self.right_expr.eval(data_chunk)?;
        let left_arr: &IA1 = left_ret.as_ref().into();
        let right_arr: &IA2 = right_ret.as_ref().into();

        let bitmap = data_chunk.get_visibility_ref();
        let mut output_array = <OA as Array>::Builder::new(data_chunk.capacity())?;
        // TODO: Consider simplify the branch below.
        Ok(Arc::new(match bitmap {
            Some(bitmap) => {
                for ((l, r), visible) in left_arr.iter().zip(right_arr.iter()).zip(bitmap.iter()) {
                    if !visible {
                        continue;
                    }
                    let output;
                    if let (Some(l), Some(r)) = (l, r) {
                        let ret = (self.func)(l, r)?;
                        output = Some(ret.as_scalar_ref());
                        output_array.append(output)?;
                    } else {
                        output_array.append(None)?;
                    }
                }
                output_array.finish()?.into()
            }
            None => {
                for (l, r) in left_arr.iter().zip(right_arr.iter()) {
                    let output;
                    if let (Some(l), Some(r)) = (l, r) {
                        let ret = (self.func)(l, r)?;
                        output = Some(ret.as_scalar_ref());
                        output_array.append(output)?;
                    } else {
                        output_array.append(None)?;
                    }
                }
                output_array.finish()?.into()
            }
        }))
    }
}

impl<
        IA1: Array,
        IA2: Array,
        OA: Array,
        F: for<'a> Fn(IA1::RefItem<'a>, IA2::RefItem<'a>) -> Result<OA::OwnedItem>
            + Sized
            + Sync
            + Send,
    > BinaryExpression<IA1, IA2, OA, F>
{
    // Compile failed due to some GAT lifetime issues so make this field private.
    // Check issues #742.
    fn new(
        left_expr: BoxedExpression,
        right_expr: BoxedExpression,
        return_type: DataTypeRef,
        func: F,
    ) -> Self {
        Self {
            left_expr,
            right_expr,
            return_type,
            func,
            data1: PhantomData,
        }
    }
}

/// The macro is responsible for specializing expressions according to the left expr and right expr.
/// Parameters:
///   l/r: the left/right child of the binary expression
///   ret: the return type of the binary expression
///   int_f/float_f: the scalar func for the binary
/// returns:
///   Boxed Expression
///
/// Note for scalar func:
/// For some scalar_function, the operations are different with different types, we can not put them in one generic function
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
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i16, i16>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I16Array, I32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i32, i32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I16Array, I64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i16, i64, i64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I16Array, F32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<i16, f32, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int16, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I16Array, F64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<i16, f64, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I32Array, I32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i32, i32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I32Array, I64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i64, i64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I32Array, F32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<i32, f32, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I32Array, F64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<i32, f64, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<I64Array, I64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i64, i64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<I64Array, F32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<i64, f32, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<I64Array, F64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<i64, f64, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<F32Array, F32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f32, f32, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<F32Array, F64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f32, f64, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Float64) => {
                Box::new(BinaryExpression::<F64Array, F64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f64, f64, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int32, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I32Array, I16Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i32, i16, i32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<I64Array, I16Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i16, i64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Int64, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<I64Array, I32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $int_f::<i64, i32, i64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<F32Array, I16Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i16, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<F32Array, I32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i32, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float32, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<F32Array, I64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f32, i64, f32>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int16) => {
                Box::new(BinaryExpression::<F64Array, I16Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i16, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int32) => {
                Box::new(BinaryExpression::<F64Array, I32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i32, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Int64) => {
                Box::new(BinaryExpression::<F64Array, I64Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f64, i64, f64>,
                    data1: PhantomData,
                })
            }
            (DataTypeKind::Float64, DataTypeKind::Float32) => {
                Box::new(BinaryExpression::<F64Array, F32Array, $OA, _> {
                    left_expr: $l,
                    right_expr: $r,
                    return_type: $ret,
                    func: $float_f::<f64, f32, f64>,
                    data1: PhantomData,
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
    expr_type: ExprNode_ExprNodeType,
    ret: DataTypeRef,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        ExprNode_ExprNodeType::EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_eq, primitive_eq)
        }
        ExprNode_ExprNodeType::NOT_EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_neq, primitive_neq)
        }
        ExprNode_ExprNodeType::LESS_THAN => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_lt, primitive_lt)
        }
        ExprNode_ExprNodeType::GREATER_THAN => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_gt, primitive_gt)
        }
        ExprNode_ExprNodeType::GREATER_THAN_OR_EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_geq, primitive_geq)
        }
        ExprNode_ExprNodeType::LESS_THAN_OR_EQUAL => {
            gen_across_binary!(l, r, ret, BoolArray, primitive_leq, primitive_leq)
        }
        _ => {
            unimplemented!(
                "The expression using vectorized expression framework is not supported yet!"
            )
        }
    }
}
