use crate::array2::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, BytesGuard, BytesWriter, DataChunk, I32Array,
    UTF8Array, UTF8ArrayBuilder,
};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{DataType, DataTypeRef};
use crate::vector_op::substr::*;
use std::marker::PhantomData;
use std::sync::Arc;

struct BinaryBytesExpression<
    IA1: Array,
    IA2: Array,
    F: for<'a> Fn(IA1::RefItem<'a>, IA2::RefItem<'a>, BytesWriter) -> Result<BytesGuard>,
> {
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    return_type: DataTypeRef,
    func: F,
    data1: PhantomData<(IA1, IA2)>,
}

impl<
        IA1: Array,
        IA2: Array,
        F: for<'a> Fn(IA1::RefItem<'a>, IA2::RefItem<'a>, BytesWriter) -> Result<BytesGuard>
            + Sized
            + Sync
            + Send,
    > Expression for BinaryBytesExpression<IA1, IA2, F>
where
    for<'a> &'a IA1: std::convert::From<&'a ArrayImpl>,
    for<'a> &'a IA2: std::convert::From<&'a ArrayImpl>,
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
        let mut output_array = UTF8ArrayBuilder::new(data_chunk.capacity())?;
        // TODO: Consider simplify the branch below.
        Ok(Arc::new(match bitmap {
            Some(bitmap) => {
                for ((l, r), visible) in left_arr.iter().zip(right_arr.iter()).zip(bitmap.iter()) {
                    if !visible {
                        continue;
                    }
                    if let (Some(l), Some(r)) = (l, r) {
                        let writer = output_array.writer();
                        let guard = (self.func)(l, r, writer)?;
                        output_array = guard.into_inner();
                    } else {
                        output_array.append(None)?;
                    }
                }
                output_array.finish()?.into()
            }
            None => {
                for (l, r) in left_arr.iter().zip(right_arr.iter()) {
                    if let (Some(l), Some(r)) = (l, r) {
                        let writer = output_array.writer();
                        let guard = (self.func)(l, r, writer)?;
                        output_array = guard.into_inner();
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
        F: for<'a> Fn(IA1::RefItem<'a>, IA2::RefItem<'a>, BytesWriter) -> Result<BytesGuard>
            + Sized
            + Sync
            + Send,
    > BinaryBytesExpression<IA1, IA2, F>
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

pub fn new_substr_start(
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryBytesExpression::<UTF8Array, I32Array, _> {
        left_expr,
        right_expr,
        return_type,
        func: substr_start,
        data1: PhantomData,
    })
}

pub fn new_substr_end(
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryBytesExpression::<UTF8Array, I32Array, _> {
        left_expr,
        right_expr,
        return_type,
        func: substr_end,
        data1: PhantomData,
    })
}
