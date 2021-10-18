use std::ops::{Add, Div, Mul, Rem, Sub};

use crate::array::PrimitiveArrayItemType;
use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::{Result, RwError};
use num_traits::{AsPrimitive, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, Float};

#[inline(always)]
pub fn integer_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedAdd,
{
    match l.as_().checked_add(&r.as_()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn float_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + Add + Float,
{
    let v = l.as_() + r.as_();
    let check = v.is_finite() && !v.is_nan();
    match check {
        true => Ok(v),
        false => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn integer_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedSub,
{
    match l.as_().checked_sub(&r.as_()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn float_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + Sub + Float,
{
    let v = l.as_() - r.as_();
    let check = v.is_finite() && !v.is_nan();
    match check {
        true => Ok(v),
        false => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn integer_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedDiv,
{
    match l.as_().checked_div(&r.as_()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn float_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + Div + Float,
{
    let v = l.as_() / r.as_();
    let check = v.is_finite() && !v.is_nan();
    match check {
        true => Ok(v),
        false => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn integer_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedMul,
{
    match l.as_().checked_mul(&r.as_()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn float_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + Mul + Float,
{
    let v = l.as_() * r.as_();
    let check = v.is_finite() && !v.is_nan();
    match check {
        true => Ok(v),
        false => Err(RwError::from(NumericValueOutOfRange)),
    }
}

pub fn primitive_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + Rem<Output = T3>,
{
    Ok(l.as_() % r.as_())
}
