use crate::array::{
    Array, ArrayBuilder, PrimitiveArray, PrimitiveArrayBuilder, PrimitiveArrayItemType,
};
use crate::error::Result;

pub fn vector_mod_primitive<T1, T2, T3>(
    a: &PrimitiveArray<T1>,
    b: &PrimitiveArray<T2>,
) -> Result<PrimitiveArray<T3>>
where
    T1: PrimitiveArrayItemType + num_traits::AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + num_traits::AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + std::ops::Rem<Output = T3>,
{
    let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len())?;
    for (a, b) in a.iter().zip(b.iter()) {
        let item = match (a, b) {
            (Some(a), Some(b)) => Some(a.as_() % b.as_()),
            _ => None,
        };
        builder.append(item)?;
    }
    builder.finish()
}
