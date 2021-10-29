use std::ops::{Add, Div, Mul, Rem, Sub};

use crate::array::PrimitiveArrayItemType;
use crate::error::ErrorCode::InternalError;
use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::{Result, RwError};
use crate::types::IntervalUnit;
use num_traits::CheckedRem;
use num_traits::{AsPrimitive, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, Float};

#[inline(always)]
pub fn int_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
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

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: CheckedAdd,
{
    match l.into().checked_add(&r.into()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn deci_f_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: CheckedAdd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert left to float".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert right to float".to_string())))?;
    match l.checked_add(&r) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn int_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
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

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: CheckedSub,
{
    match l.into().checked_sub(&r.into()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn deci_f_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: CheckedSub,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert left to float".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert right to float".to_string())))?;
    match l.checked_sub(&r) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn int_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
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

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: CheckedDiv,
{
    match l.into().checked_div(&r.into()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn deci_f_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: CheckedDiv,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert left to float".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert right to float".to_string())))?;
    match l.checked_div(&r) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn int_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
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

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: CheckedMul,
{
    match l.into().checked_mul(&r.into()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn deci_f_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: CheckedMul,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert left to float".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert right to float".to_string())))?;
    match l.checked_mul(&r) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

#[inline(always)]
pub fn prim_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + Rem<Output = T3>,
{
    Ok(l.as_() % r.as_())
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: CheckedRem,
{
    match l.into().checked_rem(&r.into()) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

pub fn deci_f_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: CheckedRem,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert left to float".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't covert right to float".to_string())))?;
    match l.checked_rem(&r) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    }
}

pub fn interval_date_add<T1, T2, T3>(l: IntervalUnit, r: T2) -> Result<T3>
where
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedAdd,
    // i32 is the return type of get_days()
    i32: AsPrimitive<T3>,
{
    int_add::<_, _, T3>(l.get_days(), r)
}

pub fn date_interval_add<T2, T1, T3>(l: T2, r: IntervalUnit) -> Result<T3>
where
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedAdd,
    // i32 is the return type of get_days()
    i32: AsPrimitive<T3>,
{
    int_add::<_, _, T3>(l, r.get_days())
}

pub fn interval_date_sub<T1, T2, T3>(l: IntervalUnit, r: T2) -> Result<T3>
where
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedSub,
    // i32 is the return type of get_days()
    i32: AsPrimitive<T3>,
{
    int_sub::<_, _, T3>(l.get_days(), r)
}

pub fn date_interval_sub<T2, T1, T3>(l: T2, r: IntervalUnit) -> Result<T3>
where
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType + CheckedSub,
    // i32 is the return type of get_days()
    i32: AsPrimitive<T3>,
{
    int_sub::<_, _, T3>(l, r.get_days())
}

#[cfg(test)]
mod tests {
    use super::deci_add;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test() {
        assert_eq!(
            deci_add::<_, _, Decimal>(Decimal::from_str("1").unwrap(), 1i32).unwrap(),
            Decimal::from_str("2").unwrap()
        );
    }
}
