use crate::array::{Array, ArrayRef, PrimitiveArray};
use crate::error::Result;
use crate::types::PrimitiveDataType;
use crate::util::downcast_ref;

pub(crate) fn vec_binary_op<A, B, IA, IB, R, F>(
    left: IA,
    right: IB,
    mut op: F,
) -> Result<Vec<Option<R>>>
where
    IA: IntoIterator<Item = Option<A>>,
    IB: IntoIterator<Item = Option<B>>,
    F: FnMut(Option<A>, Option<B>) -> Result<Option<R>>,
{
    left.into_iter()
        .zip(right)
        .map(|pair| op(pair.0, pair.1))
        .collect::<Result<Vec<Option<R>>>>()
}

pub(crate) fn vec_binary_op_primitive_array<A, B, C, F>(
    left_array: &dyn Array,
    right_array: &dyn Array,
    op: F,
) -> Result<ArrayRef>
where
    A: PrimitiveDataType,
    B: PrimitiveDataType,
    C: PrimitiveDataType,
    F: FnMut(Option<A::N>, Option<B::N>) -> Result<Option<C::N>>,
{
    let left_array: &PrimitiveArray<A> = downcast_ref(left_array)?;
    let right_array: &PrimitiveArray<B> = downcast_ref(right_array)?;

    let ret = vec_binary_op(left_array.as_iter()?, right_array.as_iter()?, op)?;
    PrimitiveArray::<C>::from_values(ret)
}
