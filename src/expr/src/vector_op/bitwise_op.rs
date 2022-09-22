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

use crate::vector_op::arithmetic_op::general_atm;
use crate::{ExprError, Result};

// Conscious decision for shl and shr is made here to diverge from PostgreSQL.
// If overflow happens, instead of truncated to zero, we return overflow error as this is
// undefined behaviour. If the RHS is negative, instead of having an unexpected answer, we return an
// error. If PG had clearly defined behavior rather than relying on UB of C, we would follow it even
// when it is different from rust std.
#[inline(always)]
pub fn general_shl<T1, T2>(l: T1, r: T2) -> Result<T1>
where
    T1: CheckedShl + Debug,
    T2: TryInto<u32> + Debug,
{
    general_shift(l, r, |a, b| {
        a.checked_shl(b).ok_or_else(|| ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
pub fn general_shr<T1, T2>(l: T1, r: T2) -> Result<T1>
where
    T1: CheckedShr + Debug,
    T2: TryInto<u32> + Debug,
{
    general_shift(l, r, |a, b| {
        a.checked_shr(b).ok_or_else(|| ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
pub fn general_shift<T1, T2, F>(l: T1, r: T2, atm: F) -> Result<T1>
where
    T1: Debug,
    T2: TryInto<u32> + Debug,
    F: FnOnce(T1, u32) -> Result<T1>,
{
    // TODO: We need to improve the error message
    let r: u32 = r
        .try_into()
        .map_err(|_| ExprError::Cast(type_name::<T2>(), type_name::<u32>()))?;
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
