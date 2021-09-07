use crate::array2::{Array, ArrayBuilder, PrimitiveArray, PrimitiveArrayBuilder};
use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::{Result, RwError};

use crate::types::NativeType;
use num_traits::cast::AsPrimitive;
use num_traits::float::Float;
use num_traits::ops::checked::CheckedSub;

/// Checked subtract two `PrimitiveArray` for integers. Both array must have the same size.
pub fn vector_sub_primitive_integer<T1, T2, T3>(
    a: PrimitiveArray<T1>,
    b: PrimitiveArray<T2>,
) -> Result<PrimitiveArray<T3>>
where
    T1: NativeType + AsPrimitive<T3>,
    T2: NativeType + AsPrimitive<T3>,
    T3: NativeType + CheckedSub,
{
    let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len())?;
    for (a, b) in a.iter().zip(b.iter()) {
        let item = match (a, b) {
            (Some(a), Some(b)) => match a.as_().checked_sub(&b.as_()) {
                Some(c) => Some(c),
                None => return Err(RwError::from(NumericValueOutOfRange)),
            },
            _ => None,
        };
        builder.append(item)?;
    }
    builder.finish()
}

/// Checked subtract two `PrimitiveArray` for floats. Both array must have the same size.
pub fn vector_sub_primitive_float<T1, T2, T3>(
    a: PrimitiveArray<T1>,
    b: PrimitiveArray<T2>,
) -> Result<PrimitiveArray<T3>>
where
    T1: NativeType + AsPrimitive<T3>,
    T2: NativeType + AsPrimitive<T3>,
    T3: NativeType + std::ops::Add + Float,
{
    let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len())?;
    for (a, b) in a.iter().zip(b.iter()) {
        let item = match (a, b) {
            (Some(a), Some(b)) => {
                let v = a.as_() - b.as_();
                let check = v.is_finite() && !v.is_nan();
                match check {
                    true => Some(v),
                    false => return Err(RwError::from(NumericValueOutOfRange)),
                }
            }
            _ => None,
        };
        builder.append(item)?;
    }
    builder.finish()
}
