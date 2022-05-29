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
use std::any::type_name;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::{BitAnd, BitOr, BitXor, Not};

use num_traits::{CheckedShl, CheckedShr};
use risingwave_common::error::ErrorCode::{InternalError, NumericValueOutOfRange};
use risingwave_common::error::{Result, RwError};

use crate::vector_op::arithmetic_op::general_atm;

#[inline(always)]
pub fn general_shl<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<u32> + Debug,
    T3: CheckedShl,
{
    general_shift(l, r, |a, b| match a.checked_shl(b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_shr<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<u32> + Debug,
    T3: CheckedShr,
{
    general_shift(l, r, |a, b| match a.checked_shr(b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_shift<T1, T2, T3, F>(l: T1, r: T2, atm: F) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<u32> + Debug,
    F: FnOnce(T3, u32) -> Result<T3>,
{
    // TODO: We need to improve the error message
    let l: T3 = l.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't convert {} to {}",
            type_name::<T1>(),
            type_name::<T3>()
        )))
    })?;
    let r: u32 = r.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't convert {} to {}",
            type_name::<T2>(),
            type_name::<u32>()
        )))
    })?;
    atm(l, r)
}

#[inline(always)]
pub fn general_bitand<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: BitAnd<Output = T3>,
{
    general_atm(l, r, |a, b| Ok(a.bitand(b)))
}

#[inline(always)]
pub fn general_bitor<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: BitOr<Output = T3>,
{
    general_atm(l, r, |a, b| Ok(a.bitor(b)))
}

#[inline(always)]
pub fn general_bitxor<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: BitXor<Output = T3>,
{
    general_atm(l, r, |a, b| Ok(a.bitxor(b)))
}

#[inline(always)]
pub fn general_bitnot<T1: Not<Output = T1>>(expr: T1) -> Result<T1> {
    Ok(expr.not())
}
   general_atm(l, r,  |a, b| Ok( a.bitxor(b)) )
}


#[inline(always)]
pub fn general_bitnot<T1: Not<Output = T1>>(expr: T1) -> Result<T1> {
    Ok(expr.not())
}

>>>>>>> Bitwise not done implemetation. TODO Testing
