use crate::array2::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I32Array, I64Array, UTF8Array,
};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{DataType, DataTypeKind, DataTypeRef, Scalar};
use crate::vector_op::cast;
use risingwave_proto::expr::ExprNode_ExprNodeType;
use std::marker::PhantomData;
use std::sync::Arc;

struct UnaryExpression<
    IA: Array,
    OA: Array,
    F: for<'a> Fn(IA::RefItem<'a>) -> Result<OA::OwnedItem> + Sized + Sync + Send,
> {
    child_expr: BoxedExpression,
    return_type: DataTypeRef,
    func: F,
    data1: PhantomData<(IA, OA)>,
}

impl<
        IA: Array,
        OA: Array,
        F: for<'a> Fn(IA::RefItem<'a>) -> Result<OA::OwnedItem> + Sized + Sync + Send,
    > Expression for UnaryExpression<IA, OA, F>
where
    for<'a> &'a IA: std::convert::From<&'a ArrayImpl>,
    for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
{
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        let eval_ret = self.child_expr.eval(data_chunk)?;
        let eval_ret_ref = eval_ret.as_ref();
        let arr: &IA = eval_ret_ref.into();
        let bitmap = data_chunk.get_visibility_ref();
        let mut output_array = <OA as Array>::Builder::new(data_chunk.cardinality())?;
        // TODO: Consider simplify the branch below.
        Ok(Arc::new(match bitmap {
            Some(bitmap) => {
                for (v, visible) in arr.iter().zip(bitmap.iter()) {
                    if !visible {
                        continue;
                    }
                    let output;
                    if let Some(non_null_v) = v {
                        let ret = (self.func)(non_null_v)?;
                        output = Some(ret.as_scalar_ref());
                        output_array.append(output)?;
                    } else {
                        output = None;
                        output_array.append(output)?;
                    }
                }
                output_array.finish()?.into()
            }
            None => {
                for v in arr.iter() {
                    let output;
                    if let Some(non_null_v) = v {
                        let ret = (self.func)(non_null_v)?;
                        output = Some(ret.as_scalar_ref());
                        output_array.append(output)?;
                    } else {
                        output = None;
                        output_array.append(output)?;
                    }
                }
                output_array.finish()?.into()
            }
        }))
    }
}

impl<
        IA: Array,
        OA: Array,
        F: for<'a> Fn(IA::RefItem<'a>) -> Result<OA::OwnedItem> + Sized + Sync + Send,
    > UnaryExpression<IA, OA, F>
{
    // Compile failed due to some GAT lifetime issues so make this field private.
    // Check issues #742.
    fn new(child_expr: BoxedExpression, return_type: DataTypeRef, func: F) -> Self {
        Self {
            child_expr,
            return_type,
            func,
            data1: PhantomData,
        }
    }
}

pub fn new_unary_expr(
    expr_type: ExprNode_ExprNodeType,
    return_type: DataTypeRef,
    child_expr: BoxedExpression,
) -> BoxedExpression {
    match (
        expr_type,
        return_type.data_type_kind(),
        child_expr.return_type().data_type_kind(),
    ) {
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Date, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I32Array, _> {
                child_expr,
                return_type,
                func: cast::str_to_date,
                data1: PhantomData,
            })
        }
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Time, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                child_expr,
                return_type,
                func: cast::str_to_time,
                data1: PhantomData,
            })
        }
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Timestamp, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                child_expr,
                return_type,
                func: cast::str_to_timestamp,
                data1: PhantomData,
            })
        }
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Timestampz, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                child_expr,
                return_type,
                func: cast::str_to_timestampz,
                data1: PhantomData,
            })
        }
        (_, _, _) => {
            unimplemented!(
                "The expression using vectorized expression framework is not supported yet!"
            )
        }
    }
}
