use crate::array::{PrimitiveArrayItemType, RwError};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use core::convert::From;
use num_traits::AsPrimitive;
use std::any::type_name;
use std::fmt::Debug;

#[inline(always)]
pub fn prim_eq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
{
    Ok(l.as_() == r.as_())
}

#[inline(always)]
pub fn prim_neq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
{
    Ok(l.as_() != r.as_())
}

#[inline(always)]
pub fn prim_leq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
{
    Ok(l.as_() <= r.as_())
}

#[inline(always)]
pub fn prim_lt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
{
    Ok(l.as_() < r.as_())
}

#[inline(always)]
pub fn prim_geq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
{
    Ok(l.as_() >= r.as_())
}

#[inline(always)]
pub fn prim_gt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
{
    Ok(l.as_() > r.as_())
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_gt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: PartialOrd,
{
    Ok(l.into() > r.into())
}

#[inline(always)]
pub fn deci_f_gt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: PartialOrd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert left to decimal".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert right to decimal".to_string())))?;
    Ok(l > r)
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_geq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: PartialOrd,
{
    Ok(l.into() >= r.into())
}

#[inline(always)]
pub fn deci_f_geq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: PartialOrd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert left to decimal".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert right to decimal".to_string())))?;
    Ok(l >= r)
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_lt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: PartialOrd,
{
    Ok(l.into() < r.into())
}

#[inline(always)]
pub fn deci_f_lt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: PartialOrd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert left to decimal".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert right to decimal".to_string())))?;
    Ok(l < r)
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_leq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: PartialOrd,
{
    Ok(l.into() <= r.into())
}

#[inline(always)]
pub fn deci_f_leq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: PartialOrd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert left to decimal".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert right to decimal".to_string())))?;
    Ok(l <= r)
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_eq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: PartialOrd,
{
    Ok(l.into() == r.into())
}

#[inline(always)]
pub fn deci_f_eq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: PartialOrd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert left to decimal".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert right to decimal".to_string())))?;
    Ok(l == r)
}

// When the input has decimal, we compare them in decimal
// T3 is decimal
#[inline(always)]
pub fn deci_neq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: Into<T3>,
    T2: Into<T3>,
    T3: PartialOrd,
{
    Ok(l.into() != r.into())
}

#[inline(always)]
pub fn deci_f_neq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3>,
    T2: TryInto<T3>,
    T3: PartialOrd,
{
    let l: T3 = l
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert left to decimal".to_string())))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| RwError::from(InternalError("Can't convert right to decimal".to_string())))?;
    Ok(l != r)
}

fn total_order_cmp<T1, T2, T3, F>(l: T1, r: T2, cmp: F) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
    F: FnOnce(T3, T3) -> bool,
{
    // TODO: We need to improve the error message
    let l: T3 = l.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't convert {} to {}",
            type_name::<T1>(),
            type_name::<T3>()
        )))
    })?;
    let r: T3 = r.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't convert {} to {}",
            type_name::<T2>(),
            type_name::<T3>()
        )))
    })?;
    Ok(cmp(l, r))
}

#[inline(always)]
pub fn total_order_eq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    total_order_cmp(l, r, |a, b| a == b)
}

#[inline(always)]
pub fn total_order_ne<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    total_order_cmp(l, r, |a, b| a != b)
}

#[inline(always)]
pub fn total_order_ge<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    total_order_cmp(l, r, |a, b| a >= b)
}

#[inline(always)]
pub fn total_order_gt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    total_order_cmp(l, r, |a, b| a > b)
}

#[inline(always)]
pub fn total_order_le<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    total_order_cmp(l, r, |a, b| a <= b)
}

#[inline(always)]
pub fn total_order_lt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    total_order_cmp(l, r, |a, b| a < b)
}

#[inline(always)]
pub fn is_true(v: Option<bool>) -> Result<Option<bool>> {
    Ok(Some(v == Some(true)))
}

#[inline(always)]
pub fn is_not_true(v: Option<bool>) -> Result<Option<bool>> {
    Ok(Some(v != Some(true)))
}

#[inline(always)]
pub fn is_false(v: Option<bool>) -> Result<Option<bool>> {
    Ok(Some(v == Some(false)))
}

#[inline(always)]
pub fn is_not_false(v: Option<bool>) -> Result<Option<bool>> {
    Ok(Some(v != Some(false)))
}

#[inline(always)]
pub fn is_unknown(v: Option<bool>) -> Result<Option<bool>> {
    Ok(Some(v.is_none()))
}

#[inline(always)]
pub fn is_not_unknown(v: Option<bool>) -> Result<Option<bool>> {
    Ok(Some(v.is_some()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_deci_f() {
        assert!(deci_f_eq::<_, _, f32>(Decimal::from_str("1.1").unwrap(), 1.1f32).unwrap())
    }
}
