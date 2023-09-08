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

use risingwave_common::array::{ListRef};
use risingwave_common::types::{Scalar, ScalarImpl, ScalarRefImpl};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

// #[function("array_sum(list) -> *float")]
// #[function("array_sum(list) -> decimal")]
// #[function("array_sum(list) -> serial")]
// #[function("array_sum(list) -> int256")]
// #[function("array_sum(list) -> date")]
// #[function("array_sum(list) -> time")]
// #[function("array_sum(list) -> timestamp")]
// #[function("array_sum(list) -> timestamptz")]
// #[function("array_sum(list) -> varchar")]
// #[function("array_sum(list) -> bytea")]
#[function("array_sum(list) -> *int")]
pub fn array_sum<T: Scalar>(list: ListRef<'_>) -> Result<Option<T>> {
    let flag = match list.iter().flatten().next() {
        Some(v) => match v {
            ScalarRefImpl::Int16(_) => 0,
            ScalarRefImpl::Int32(_) => 1,
            ScalarRefImpl::Int64(_) => 2,
            _ => -1,
        },
        None => -1,
    };

    match flag {
        0 => {
            let mut ret = 0;
            for e in list.iter().flatten() {
                if let ScalarRefImpl::Int16(v) = e {
                    ret += v;
                } else {
                    panic!("Expected ScalarRefImpl::Int16");
                }
            }
            Ok(Some(ScalarImpl::from(ret).try_into()?))
        }
        1 => {
            let mut ret = 0;
            for e in list.iter().flatten() {
                if let ScalarRefImpl::Int32(v) = e {
                    ret += v;
                } else {
                    panic!("Expected ScalarRefImpl::Int32");
                }
            }
            Ok(Some(ScalarImpl::from(ret).try_into()?))
        }
        2 => {
            let mut ret = 0;
            for e in list.iter().flatten() {
                if let ScalarRefImpl::Int64(v) = e {
                    ret += v;
                } else {
                    panic!("Expected ScalarRefImpl::Int64");
                }
            }
            Ok(Some(ScalarImpl::from(ret).try_into()?))
        }
        _ => Err(ExprError::NumericOutOfRange),
    }
}
