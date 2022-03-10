use crate::array::Array;
use crate::error::{ErrorCode, Result};

/// Essentially `RTFn` is an alias of the specific Fn. It was aliased not to
/// shorten the `where` clause of `GeneralAgg`, but to workaround an compiler
/// error[E0582]: binding for associated type `Output` references lifetime `'a`,
/// which does not appear in the trait input types.
pub trait RTFn<'a, T, R> = Send
    + 'static
    + Fn(
        Option<<R as Array>::RefItem<'a>>,
        Option<<T as Array>::RefItem<'a>>,
    ) -> Option<<R as Array>::RefItem<'a>>
where
    T: Array,
    R: Array;

use std::convert::From;
use std::ops::Add;

use crate::types::ScalarRef;

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

/// create table t(v1 int);
/// insert into t values (null);
/// select count(*) from t; gives 1.
/// select count(v1) from t; gives 0.
/// select sum(v1) from t; gives null
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

pub fn single_value<'a, T>(result: Option<T>, input: Option<T>) -> Result<Option<T>>
where
    T: ScalarRef<'a> + PartialOrd,
{
    match (result, input) {
        (None, _) => Ok(input),
        (Some(_), None) => Ok(result),
        (Some(_), Some(_)) => Err(ErrorCode::InternalError(
            "SingleValue aggregation can only accept exactly one value. But there is more than one.".to_string(),
        )
        .into()),
    }
}

pub fn single_value_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Result<Option<&'a str>> {
    single_value(r, i)
}
