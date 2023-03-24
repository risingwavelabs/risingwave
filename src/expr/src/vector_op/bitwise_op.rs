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
use std::any::type_name;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::{BitAnd, BitOr, BitXor, Not};

use num_traits::{CheckedShl, CheckedShr};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

// Conscious decision for shl and shr is made here to diverge from PostgreSQL.
// If overflow happens, instead of truncated to zero, we return overflow error as this is
// undefined behaviour. If the RHS is negative, instead of having an unexpected answer, we return an
// error. If PG had clearly defined behavior rather than relying on UB of C, we would follow it even
// when it is different from rust std.
#[function("bitwise_shift_left(int16, int16) -> int16")]
#[function("bitwise_shift_left(int16, int32) -> int16")]
#[function("bitwise_shift_left(int32, int16) -> int32")]
#[function("bitwise_shift_left(int32, int32) -> int32")]
#[function("bitwise_shift_left(int64, int16) -> int64")]
#[function("bitwise_shift_left(int64, int32) -> int64")]
pub fn general_shl<T1, T2>(l: T1, r: T2) -> Result<T1>
where
    T1: CheckedShl + Debug,
    T2: TryInto<u32> + Debug,
{
    general_shift(l, r, |a, b| {
        a.checked_shl(b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[function("bitwise_shift_right(int16, int16) -> int16")]
#[function("bitwise_shift_right(int16, int32) -> int16")]
#[function("bitwise_shift_right(int32, int16) -> int32")]
#[function("bitwise_shift_right(int32, int32) -> int32")]
#[function("bitwise_shift_right(int64, int16) -> int64")]
#[function("bitwise_shift_right(int64, int32) -> int64")]
pub fn general_shr<T1, T2>(l: T1, r: T2) -> Result<T1>
where
    T1: CheckedShr + Debug,
    T2: TryInto<u32> + Debug,
{
    general_shift(l, r, |a, b| {
        a.checked_shr(b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
fn general_shift<T1, T2, F>(l: T1, r: T2, atm: F) -> Result<T1>
where
    T1: Debug,
    T2: TryInto<u32> + Debug,
    F: FnOnce(T1, u32) -> Result<T1>,
{
    // TODO: We need to improve the error message
    let r: u32 = r
        .try_into()
        .map_err(|_| ExprError::CastOutOfRange(type_name::<u32>()))?;
    atm(l, r)
}

#[function("bitwise_and(*int, *int) -> auto")]
pub fn general_bitand<T1, T2, T3>(l: T1, r: T2) -> T3
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: BitAnd<Output = T3>,
{
    l.into() & r.into()
}

#[function("bitwise_or(*int, *int) -> auto")]
pub fn general_bitor<T1, T2, T3>(l: T1, r: T2) -> T3
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: BitOr<Output = T3>,
{
    l.into() | r.into()
}

#[function("bitwise_xor(*int, *int) -> auto")]
pub fn general_bitxor<T1, T2, T3>(l: T1, r: T2) -> T3
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: BitXor<Output = T3>,
{
    l.into() ^ r.into()
}

#[function("bitwise_not(*int) -> auto")]
pub fn general_bitnot<T1: Not<Output = T1>>(expr: T1) -> T1 {
    !expr
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_bitwise() {
        // check the boundary
        assert_eq!(general_shl::<i32, i32>(1i32, 0i32).unwrap(), 1i32);
        assert_eq!(general_shl::<i64, i32>(1i64, 31i32).unwrap(), 2147483648i64);
        assert_matches!(
            general_shl::<i32, i32>(1i32, 32i32).unwrap_err(),
            ExprError::NumericOutOfRange,
        );
        assert_eq!(
            general_shr::<i64, i32>(-2147483648i64, 31i32).unwrap(),
            -1i64
        );
        assert_eq!(general_shr::<i64, i32>(1i64, 0i32).unwrap(), 1i64);
        // truth table
        assert_eq!(
            general_bitand::<u32, u32, u64>(0b0011u32, 0b0101u32),
            0b1u64
        );
        assert_eq!(
            general_bitor::<u32, u32, u64>(0b0011u32, 0b0101u32),
            0b0111u64
        );
        assert_eq!(
            general_bitxor::<u32, u32, u64>(0b0011u32, 0b0101u32),
            0b0110u64
        );
        assert_eq!(general_bitnot::<i32>(0b01i32), -2i32);
    }
}
