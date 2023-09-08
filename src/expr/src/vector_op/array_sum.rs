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

use risingwave_common::array::{ArrayError, ListRef};
use risingwave_common::types::{CheckedAdd, ScalarRefImpl};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

#[function("array_sum(list) -> *int")]
#[function("array_sum(list) -> *float")]
#[function("array_sum(list) -> decimal")]
#[function("array_sum(list) -> interval")]
fn array_sum<T>(list: ListRef<'_>) -> Result<T>
where
    T: Default + for<'a> TryFrom<ScalarRefImpl<'a>, Error = ArrayError> + CheckedAdd<Output = T>,
{
    let mut sum = T::default();
    for e in list.iter().flatten() {
        let v = e.try_into()?;
        sum = sum
            .checked_add(v)
            .ok_or_else(|| ExprError::NumericOutOfRange)?;
    }
    Ok(sum)
}
