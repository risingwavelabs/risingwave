// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;

use risingwave_common::array::{ListRef, StructRef};

use crate::Result;

#[inline(always)]
pub fn general_eq<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() == r.into()
}

#[inline(always)]
pub fn general_ne<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() != r.into()
}

#[inline(always)]
pub fn general_ge<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() >= r.into()
}

#[inline(always)]
pub fn general_gt<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() > r.into()
}

#[inline(always)]
pub fn general_le<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() <= r.into()
}

#[inline(always)]
pub fn general_lt<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() < r.into()
}

pub fn general_is_distinct_from<T1, T2, T3>(l: Option<T1>, r: Option<T2>) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    match (l, r) {
        (Some(lv), Some(rv)) => general_ne::<T1, T2, T3>(lv, rv),
        (Some(_), None) => true,
        (None, Some(_)) => true,
        (None, None) => false,
    }
}

pub fn general_is_not_distinct_from<T1, T2, T3>(l: Option<T1>, r: Option<T2>) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    match (l, r) {
        (Some(lv), Some(rv)) => general_eq::<T1, T2, T3>(lv, rv),
        (Some(_), None) => false,
        (None, Some(_)) => false,
        (None, None) => true,
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

pub(crate) static EQ: Comparison = Comparison::Eq;
pub(crate) static NE: Comparison = Comparison::Ne;
pub(crate) static LT: Comparison = Comparison::Lt;
pub(crate) static GT: Comparison = Comparison::Gt;
pub(crate) static LE: Comparison = Comparison::Le;
pub(crate) static GE: Comparison = Comparison::Ge;

#[inline(always)]
pub fn gen_struct_cmp(op: Comparison) -> fn(StructRef<'_>, StructRef<'_>) -> Result<bool> {
    use crate::gen_cmp;
    gen_cmp!(op)
}

#[inline(always)]
pub fn gen_list_cmp(op: Comparison) -> fn(ListRef<'_>, ListRef<'_>) -> Result<bool> {
    use crate::gen_cmp;
    gen_cmp!(op)
}

#[inline(always)]
pub fn gen_str_cmp(op: Comparison) -> fn(&str, &str) -> Result<bool> {
    use crate::gen_cmp;
    gen_cmp!(op)
}

#[macro_export]
macro_rules! gen_cmp {
    ($op:expr) => {
        match $op {
            Comparison::Eq => |l, r| Ok(l == r),
            Comparison::Ne => |l, r| Ok(l != r),
            Comparison::Lt => |l, r| Ok(l < r),
            Comparison::Gt => |l, r| Ok(l > r),
            Comparison::Le => |l, r| Ok(l <= r),
            Comparison::Ge => |l, r| Ok(l >= r),
        }
    };
}

pub fn str_is_distinct_from(l: Option<&str>, r: Option<&str>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(lv), Some(rv)) => Ok(Some(lv != rv)),
        (Some(_), None) => Ok(Some(true)),
        (None, Some(_)) => Ok(Some(true)),
        (None, None) => Ok(Some(false)),
    }
}

pub fn str_is_not_distinct_from(l: Option<&str>, r: Option<&str>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(lv), Some(rv)) => Ok(Some(lv == rv)),
        (Some(_), None) => Ok(Some(false)),
        (None, Some(_)) => Ok(Some(false)),
        (None, None) => Ok(Some(true)),
    }
}

#[inline(always)]
pub fn is_true(v: Option<bool>) -> Option<bool> {
    Some(v == Some(true))
}

#[inline(always)]
pub fn is_not_true(v: Option<bool>) -> Option<bool> {
    Some(v != Some(true))
}

#[inline(always)]
pub fn is_false(v: Option<bool>) -> Option<bool> {
    Some(v == Some(false))
}

#[inline(always)]
pub fn is_not_false(v: Option<bool>) -> Option<bool> {
    Some(v != Some(false))
}

#[inline(always)]
pub fn is_unknown(v: Option<bool>) -> Option<bool> {
    Some(v.is_none())
}

#[inline(always)]
pub fn is_not_unknown(v: Option<bool>) -> Option<bool> {
    Some(v.is_some())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::types::Decimal;

    use super::*;

    #[test]
    fn test_deci_f() {
        assert!(general_eq::<_, _, Decimal>(
            Decimal::from_str("1.1").unwrap(),
            1.1f32
        ))
    }
}
