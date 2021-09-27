use crate::array2::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, BytesGuard, BytesWriter, DataChunk, I32Array,
    UTF8Array, UTF8ArrayBuilder,
};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{DataType, DataTypeRef};
use crate::vector_op::substr::substr_start_end;
use std::marker::PhantomData;
use std::sync::Arc;

struct TenaryBytesExpression<
    IA1: Array,
    IA2: Array,
    IA3: Array,
    F: for<'a> Fn(
        IA1::RefItem<'a>,
        IA2::RefItem<'a>,
        IA3::RefItem<'a>,
        BytesWriter,
    ) -> Result<BytesGuard>,
> {
    exprs: (BoxedExpression, BoxedExpression, BoxedExpression),
    return_type: DataTypeRef,
    func: F,
    data1: PhantomData<(IA1, IA2, IA3)>,
}

impl<
        IA1: Array,
        IA2: Array,
        IA3: Array,
        F: for<'a> Fn(
                IA1::RefItem<'a>,
                IA2::RefItem<'a>,
                IA3::RefItem<'a>,
                BytesWriter,
            ) -> Result<BytesGuard>
            + Sized
            + Sync
            + Send,
    > Expression for TenaryBytesExpression<IA1, IA2, IA3, F>
where
    for<'a> &'a IA1: std::convert::From<&'a ArrayImpl>,
    for<'a> &'a IA2: std::convert::From<&'a ArrayImpl>,
    for<'a> &'a IA3: std::convert::From<&'a ArrayImpl>,
{
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        let ret1 = self.exprs.0.eval(data_chunk)?;
        let ret2 = self.exprs.1.eval(data_chunk)?;
        let ret3 = self.exprs.2.eval(data_chunk)?;
        let arr1: &IA1 = ret1.as_ref().into();
        let arr2: &IA2 = ret2.as_ref().into();
        let arr3: &IA3 = ret3.as_ref().into();

        let bitmap = data_chunk.get_visibility_ref();
        let mut output_array = UTF8ArrayBuilder::new(data_chunk.capacity())?;
        // TODO: Consider simplify the branch below.
        Ok(Arc::new(match bitmap {
            Some(bitmap) => {
                for (((v1, v2), v3), visible) in arr1
                    .iter()
                    .zip(arr2.iter())
                    .zip(arr3.iter())
                    .zip(bitmap.iter())
                {
                    if !visible {
                        continue;
                    }
                    if let (Some(v1), Some(v2), Some(v3)) = (v1, v2, v3) {
                        let writer = output_array.writer();
                        let guard = (self.func)(v1, v2, v3, writer)?;
                        output_array = guard.into_inner();
                    } else {
                        output_array.append(None)?;
                    }
                }
                output_array.finish()?.into()
            }
            None => {
                for ((v1, v2), v3) in arr1.iter().zip(arr2.iter()).zip(arr3.iter()) {
                    if let (Some(v1), Some(v2), Some(v3)) = (v1, v2, v3) {
                        let writer = output_array.writer();
                        let guard = (self.func)(v1, v2, v3, writer)?;
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
        IA3: Array,
        F: for<'a> Fn(
                IA1::RefItem<'a>,
                IA2::RefItem<'a>,
                IA3::RefItem<'a>,
                BytesWriter,
            ) -> Result<BytesGuard>
            + Sized
            + Sync
            + Send,
    > TenaryBytesExpression<IA1, IA2, IA3, F>
{
    // Compile failed due to some GAT lifetime issues so make this field private.
    // Check issues #742.
    fn new(
        expr1: BoxedExpression,
        expr2: BoxedExpression,
        expr3: BoxedExpression,
        return_type: DataTypeRef,
        func: F,
    ) -> Self {
        Self {
            exprs: (expr1, expr2, expr3),
            return_type,
            func,
            data1: PhantomData,
        }
    }
}

pub fn new_substr_start_end(
    items: BoxedExpression,
    off: BoxedExpression,
    len: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(TenaryBytesExpression::<UTF8Array, I32Array, I32Array, _> {
        exprs: (items, off, len),
        return_type,
        func: substr_start_end,
        data1: PhantomData,
    })
}
