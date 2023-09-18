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
use risingwave_common::types::{CheckedAdd, Decimal, Scalar, ScalarImpl, ScalarRefImpl};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

/// `array_sum(int16`[]) -> int64
/// `array_sum(int32`[]) -> int64
#[function("array_sum(list) -> int64")]
#[function("array_sum(list) -> float32")]
#[function("array_sum(list) -> float64")]
/// `array_sum(int64`[]) -> decimal
/// `array_sum(decimal`[]) -> decimal
#[function("array_sum(list) -> decimal")]
#[function("array_sum(list) -> interval")]
fn array_sum<T: Scalar>(list: ListRef<'_>) -> Result<Option<T>>
where
    T: Default + for<'a> TryFrom<ScalarRefImpl<'a>, Error = ArrayError> + CheckedAdd<Output = T>,
{
    let flag = match list.iter().flatten().next() {
        Some(v) => match v {
            ScalarRefImpl::Int16(_) | ScalarRefImpl::Int32(_) => 1,
            ScalarRefImpl::Int64(_) => 2,
            _ => 0,
        },
        None => return Ok(None),
    };

    if flag != 0 {
        match flag {
            1 => {
                let mut sum = 0;
                for e in list.iter().flatten() {
                    sum = sum
                        .checked_add(match e {
                            ScalarRefImpl::Int16(v) => v as i64,
                            ScalarRefImpl::Int32(v) => v as i64,
                            _ => panic!("Expect ScalarRefImpl::Int16 or ScalarRefImpl::Int32"),
                        })
                        .ok_or_else(|| ExprError::NumericOutOfRange)?;
                }
                Ok(Some(ScalarImpl::from(sum).try_into()?))
            }
            2 => {
                let mut sum = Decimal::Normalized(0.into());
                for e in list.iter().flatten() {
                    sum = sum
                        .checked_add(match e {
                            ScalarRefImpl::Int64(v) => Decimal::Normalized(v.into()),
                            ScalarRefImpl::Decimal(v) => v,
                            // FIXME: We can't panic here due to the macro expansion
                            _ => Decimal::Normalized(0.into()),
                        })
                        .ok_or_else(|| ExprError::NumericOutOfRange)?;
                }
                Ok(Some(ScalarImpl::from(sum).try_into()?))
            }
            _ => Ok(None),
        }
    } else {
        let mut sum = T::default();
        for e in list.iter().flatten() {
            let v = e.try_into()?;
            sum = sum
                .checked_add(v)
                .ok_or_else(|| ExprError::NumericOutOfRange)?;
        }
        Ok(Some(sum))
    }
}
