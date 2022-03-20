// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use core::convert::From;
use std::any::type_name;
use std::fmt::Debug;

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

fn general_cmp<T1, T2, T3, F>(l: T1, r: T2, cmp: F) -> Result<bool>
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
pub fn general_eq<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    general_cmp(l, r, |a, b| a == b)
}

#[inline(always)]
pub fn general_ne<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    general_cmp(l, r, |a, b| a != b)
}

#[inline(always)]
pub fn general_ge<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    general_cmp(l, r, |a, b| a >= b)
}

#[inline(always)]
pub fn general_gt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    general_cmp(l, r, |a, b| a > b)
}

#[inline(always)]
pub fn general_le<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    general_cmp(l, r, |a, b| a <= b)
}

#[inline(always)]
pub fn general_lt<T1, T2, T3>(l: T1, r: T2) -> Result<bool>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: Ord,
{
    general_cmp(l, r, |a, b| a < b)
}

#[inline(always)]
fn str_cmp<F>(l: &str, r: &str, func: F) -> Result<bool>
where
    F: FnOnce(&str, &str) -> bool,
{
    Ok(func(l, r))
}

#[inline(always)]
pub fn str_eq(l: &str, r: &str) -> Result<bool> {
    str_cmp(l, r, |a, b| a == b)
}

#[inline(always)]
pub fn str_ne(l: &str, r: &str) -> Result<bool> {
    str_cmp(l, r, |a, b| a != b)
}

#[inline(always)]
pub fn str_ge(l: &str, r: &str) -> Result<bool> {
    str_cmp(l, r, |a, b| a >= b)
}

#[inline(always)]
pub fn str_gt(l: &str, r: &str) -> Result<bool> {
    str_cmp(l, r, |a, b| a > b)
}

#[inline(always)]
pub fn str_le(l: &str, r: &str) -> Result<bool> {
    str_cmp(l, r, |a, b| a <= b)
}

#[inline(always)]
pub fn str_lt(l: &str, r: &str) -> Result<bool> {
    str_cmp(l, r, |a, b| a < b)
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
    use std::str::FromStr;

    use super::*;
    use crate::types::Decimal;

    #[test]
    fn test_deci_f() {
        assert!(general_eq::<_, _, Decimal>(Decimal::from_str("1.1").unwrap(), 1.1f32).unwrap())
    }
}
