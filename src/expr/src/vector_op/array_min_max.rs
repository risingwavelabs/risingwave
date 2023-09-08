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

use risingwave_common::array::*;
use risingwave_common::types::{DefaultOrdered, Scalar, ToOwnedDatum};
use risingwave_expr_macro::function;

use crate::Result;

#[function("array_min(int16[]) -> int16")]
#[function("array_min(int32[]) -> int32")]
#[function("array_min(int64[]) -> int64")]
#[function("array_min(float32[]) -> float32")]
#[function("array_min(float64[]) -> float64")]
#[function("array_min(decimal[]) -> decimal")]
#[function("array_min(serial[]) -> serial")]
#[function("array_min(int256[]) -> int256")]
#[function("array_min(date[]) -> date")]
#[function("array_min(time[]) -> time")]
#[function("array_min(timestamp[]) -> timestamp")]
#[function("array_min(timestamptz[]) -> timestamptz")]
#[function("array_min(varchar[]) -> varchar")]
#[function("array_min(bytea[]) -> bytea")]
pub fn array_min(list: ListRef<'_>) -> Result<Option<impl Scalar>> {
    let min_value = list.iter().flatten().map(DefaultOrdered).min();
    match min_value.map(|v| v.0).to_owned_datum() {
        Some(s) => Ok(Some(s.try_into()?)),
        None => Ok(None),
    }
}

#[function("array_max(list) -> *int")]
#[function("array_max(list) -> *float")]
#[function("array_max(list) -> decimal")]
#[function("array_max(list) -> serial")]
#[function("array_max(list) -> int256")]
#[function("array_max(list) -> date")]
#[function("array_max(list) -> time")]
#[function("array_max(list) -> timestamp")]
#[function("array_max(list) -> timestamptz")]
#[function("array_max(list) -> varchar")]
#[function("array_max(list) -> bytea")]
pub fn array_max<T: Scalar>(list: ListRef<'_>) -> Result<Option<T>> {
    let max_value = list.iter().flatten().map(DefaultOrdered).max();
    match max_value.map(|v| v.0).to_owned_datum() {
        Some(s) => Ok(Some(s.try_into()?)),
        None => Ok(None),
    }
}
