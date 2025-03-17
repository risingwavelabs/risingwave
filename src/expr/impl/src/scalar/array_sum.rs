// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::{ArrayError, ListRef};
use risingwave_common::types::{CheckedAdd, Decimal, ScalarRefImpl};
use risingwave_expr::{ExprError, Result, function};

#[function("array_sum(int2[]) -> int8")]
fn array_sum_int2(list: ListRef<'_>) -> Result<Option<i64>> {
    array_sum_general::<i16, i64>(list)
}

#[function("array_sum(int4[]) -> int8")]
fn array_sum_int4(list: ListRef<'_>) -> Result<Option<i64>> {
    array_sum_general::<i32, i64>(list)
}

#[function("array_sum(int8[]) -> decimal")]
fn array_sum_int8(list: ListRef<'_>) -> Result<Option<Decimal>> {
    array_sum_general::<i64, Decimal>(list)
}

#[function("array_sum(float4[]) -> float4")]
#[function("array_sum(float8[]) -> float8")]
#[function("array_sum(decimal[]) -> decimal")]
#[function("array_sum(interval[]) -> interval")]
fn array_sum<T>(list: ListRef<'_>) -> Result<Option<T>>
where
    T: for<'a> TryFrom<ScalarRefImpl<'a>, Error = ArrayError>,
    T: Default + From<T> + CheckedAdd<Output = T>,
{
    array_sum_general::<T, T>(list)
}

fn array_sum_general<S, T>(list: ListRef<'_>) -> Result<Option<T>>
where
    S: for<'a> TryFrom<ScalarRefImpl<'a>, Error = ArrayError>,
    T: Default + From<S> + CheckedAdd<Output = T>,
{
    if list.iter().flatten().next().is_none() {
        return Ok(None);
    }
    let mut sum = T::default();
    for e in list.iter().flatten() {
        let v: S = e.try_into()?;
        sum = sum
            .checked_add(v.into())
            .ok_or_else(|| ExprError::NumericOutOfRange)?;
    }
    Ok(Some(sum))
}
