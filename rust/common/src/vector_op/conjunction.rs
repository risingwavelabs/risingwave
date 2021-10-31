use crate::error::Result;

#[inline(always)]
pub fn and(l: bool, r: bool) -> Result<bool> {
    Ok(l && r)
}

#[inline(always)]
pub fn or(l: bool, r: bool) -> Result<bool> {
    Ok(l || r)
}

#[inline(always)]
pub fn not(l: bool) -> Result<bool> {
    Ok(!l)
}
