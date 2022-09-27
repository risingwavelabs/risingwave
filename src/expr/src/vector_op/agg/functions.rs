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

use risingwave_common::array::{Array, ListRef, StructRef};

use crate::Result;

/// Essentially `RTFn` is an alias of the specific Fn. It was aliased not to
/// shorten the `where` clause of `GeneralAgg`, but to workaround an compiler
/// error`[E0582`]: binding for associated type `Output` references lifetime `'a`,
/// which does not appear in the trait input types.
pub trait RTFn<'a, T, R>: Send + Clone + 'static
where
    T: Array,
    R: Array,
{
    fn eval(
        &mut self,
        result: Option<<R as Array>::RefItem<'a>>,
        input: Option<<T as Array>::RefItem<'a>>,
    ) -> Result<Option<<R as Array>::RefItem<'a>>>;
}

impl<'a, T, R, Z> RTFn<'a, T, R> for Z
where
    T: Array,
    R: Array,
    Z: Send
        + Clone
        + 'static
        + Fn(
            Option<<R as Array>::RefItem<'a>>,
            Option<<T as Array>::RefItem<'a>>,
        ) -> Result<Option<<R as Array>::RefItem<'a>>>,
{
    fn eval(
        &mut self,
        result: Option<<R as Array>::RefItem<'a>>,
        input: Option<<T as Array>::RefItem<'a>>,
    ) -> Result<Option<<R as Array>::RefItem<'a>>> {
        self.call((result, input))
    }
}

use std::convert::From;
use std::ops::Add;

use risingwave_common::types::ScalarRef;

pub fn sum<R, T>(result: Option<R>, input: Option<T>) -> Result<Option<R>>
where
    R: From<T> + Add<Output = R> + Copy,
{
    let res = match (result, input) {
        (_, None) => result,
        (None, Some(i)) => Some(R::from(i)),
        (Some(r), Some(i)) => Some(r + R::from(i)),
    };
    Ok(res)
}

pub fn min<'a, T>(result: Option<T>, input: Option<T>) -> Result<Option<T>>
where
    T: ScalarRef<'a> + PartialOrd,
{
    let res = match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(if r < i { r } else { i }),
    };
    Ok(res)
}

pub fn min_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Result<Option<&'a str>> {
    min(r, i)
}

pub fn min_struct<'a>(
    r: Option<StructRef<'a>>,
    i: Option<StructRef<'a>>,
) -> Result<Option<StructRef<'a>>> {
    min(r, i)
}

pub fn min_list<'a>(r: Option<ListRef<'a>>, i: Option<ListRef<'a>>) -> Result<Option<ListRef<'a>>> {
    min(r, i)
}

pub fn max<'a, T>(result: Option<T>, input: Option<T>) -> Result<Option<T>>
where
    T: ScalarRef<'a> + PartialOrd,
{
    let res = match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(if r > i { r } else { i }),
    };
    Ok(res)
}

pub fn max_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Result<Option<&'a str>> {
    max(r, i)
}

pub fn max_struct<'a>(
    r: Option<StructRef<'a>>,
    i: Option<StructRef<'a>>,
) -> Result<Option<StructRef<'a>>> {
    max(r, i)
}

pub fn max_list<'a>(r: Option<ListRef<'a>>, i: Option<ListRef<'a>>) -> Result<Option<ListRef<'a>>> {
    max(r, i)
}

/// Note the following corner cases:
///
/// ```slt
/// statement ok
/// create table t(v1 int);
///
/// statement ok
/// insert into t values (null);
///
/// query I
/// select count(*) from t;
/// ----
/// 1
///
/// query I
/// select count(v1) from t;
/// ----
/// 0
///
/// query I
/// select sum(v1) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
pub fn count<T>(result: Option<i64>, input: Option<T>) -> Result<Option<i64>> {
    let res = match (result, input) {
        (None, None) => Some(0),
        (Some(r), None) => Some(r),
        (None, Some(_)) => Some(1),
        (Some(r), Some(_)) => Some(r + 1),
    };
    Ok(res)
}

pub fn count_str(r: Option<i64>, i: Option<&str>) -> Result<Option<i64>> {
    count(r, i)
}

pub fn count_struct(r: Option<i64>, i: Option<StructRef<'_>>) -> Result<Option<i64>> {
    count(r, i)
}

pub fn count_list(r: Option<i64>, i: Option<ListRef<'_>>) -> Result<Option<i64>> {
    count(r, i)
}
