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

use std::convert::From;

use num_traits::CheckedAdd;

use crate::{ExprError, Result};

#[aggregate("sum(int16) -> int64")]
#[aggregate("sum(int32) -> int64")]
#[aggregate("sum(int64) -> int64")]
#[aggregate("sum(int64) -> decimal")]
#[aggregate("sum(float32) -> float32")]
#[aggregate("sum(float64) -> float64")]
#[aggregate("sum(decimal) -> decimal")]
#[aggregate("sum(interval) -> interval")]
fn sum<R, T>(result: Option<R>, input: Option<T>) -> Result<Option<R>>
where
    R: From<T> + CheckedAdd<Output = R> + Copy,
{
    Ok(match (result, input) {
        (_, None) => result,
        (None, Some(i)) => Some(R::from(i)),
        (Some(r), Some(i)) => r
            .checked_add(&R::from(i))
            .map_or(Err(ExprError::NumericOutOfRange), |x| Ok(Some(x)))?,
    })
}

#[aggregate("min(*) -> auto")]
fn min<T: Ord>(result: Option<T>, input: Option<T>) -> Option<T> {
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(r.min(i)),
    }
}

#[aggregate("max(*) -> auto")]
fn max<T: Ord>(result: Option<T>, input: Option<T>) -> Option<T> {
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(r.max(i)),
    }
}

#[aggregate("first_value(*) -> auto")]
fn first<T>(result: Option<T>, input: Option<T>) -> Option<T> {
    result.or(input)
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
#[aggregate("count(*) -> int64")]
fn count<T>(result: Option<i64>, input: Option<T>) -> Option<i64> {
    match (result, input) {
        (None, None) => Some(0),
        (Some(r), None) => Some(r),
        (None, Some(_)) => Some(1),
        (Some(r), Some(_)) => Some(r + 1),
    }
}
