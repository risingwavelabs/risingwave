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

use std::assert_matches::assert_matches;
use std::str::FromStr;

use chrono::NaiveDateTime;
use risingwave_common::types::test_utils::IntervalUnitTestExt;
use risingwave_common::types::{
    Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, OrderedF32, OrderedF64,
};

use crate::vector_op::arithmetic_op::*;
use crate::vector_op::bitwise_op::*;
use crate::vector_op::cast::try_cast;
use crate::vector_op::cmp::*;
use crate::vector_op::conjunction::*;
use crate::ExprError;

#[test]
fn test_arithmetic() {
    assert_eq!(
        general_add::<Decimal, i32, Decimal>(dec("1.0"), 1).unwrap(),
        dec("2.0")
    );
    assert_eq!(
        general_sub::<Decimal, i32, Decimal>(dec("1.0"), 2).unwrap(),
        dec("-1.0")
    );
    assert_eq!(
        general_mul::<Decimal, i32, Decimal>(dec("1.0"), 2).unwrap(),
        dec("2.0")
    );
    assert_eq!(
        general_div::<Decimal, i32, Decimal>(dec("2.0"), 2).unwrap(),
        dec("1.0")
    );
    assert_eq!(
        general_mod::<Decimal, i32, Decimal>(dec("2.0"), 2).unwrap(),
        dec("0")
    );
    assert_eq!(general_neg::<Decimal>(dec("1.0")).unwrap(), dec("-1.0"));
    assert_eq!(general_add::<i16, i32, i32>(1i16, 1i32).unwrap(), 2i32);
    assert_eq!(general_sub::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(general_mul::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(general_div::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(general_mod::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(general_neg::<i16>(1i16).unwrap(), -1i16);

    assert_eq!(
        general_add::<Decimal, f32, Decimal>(dec("1.0"), -1f32).unwrap(),
        dec("0.0")
    );
    assert_eq!(
        general_sub::<Decimal, f32, Decimal>(dec("1.0"), 1f32).unwrap(),
        dec("0.0")
    );
    assert_eq!(
        general_div::<Decimal, f32, Decimal>(dec("0.0"), 1f32).unwrap(),
        dec("0.0")
    );
    assert_eq!(
        general_mul::<Decimal, f32, Decimal>(dec("0.0"), 1f32).unwrap(),
        dec("0.0")
    );
    assert_eq!(
        general_mod::<Decimal, f32, Decimal>(dec("0.0"), 1f32).unwrap(),
        dec("0.0")
    );
    assert!(
        general_add::<i32, OrderedF32, OrderedF64>(-1i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert!(
        general_sub::<i32, OrderedF32, OrderedF64>(1i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert!(
        general_mul::<i32, OrderedF32, OrderedF64>(0i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert!(
        general_div::<i32, OrderedF32, OrderedF64>(0i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert_eq!(
        general_neg::<OrderedF32>(1f32.into()).unwrap(),
        OrderedF32::from(-1f32)
    );
    assert_eq!(
        date_interval_add(
            NaiveDateWrapper::from_ymd_uncheck(1994, 1, 1),
            IntervalUnit::from_month(12)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
    assert_eq!(
        interval_date_add(
            IntervalUnit::from_month(12),
            NaiveDateWrapper::from_ymd_uncheck(1994, 1, 1)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
    assert_eq!(
        date_interval_sub(
            NaiveDateWrapper::from_ymd_uncheck(1994, 1, 1),
            IntervalUnit::from_month(12)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1993-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
}

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

#[test]
fn test_comparison() {
    assert!(general_eq::<Decimal, i32, Decimal>(dec("1.0"), 1));
    assert!(general_eq::<Decimal, f32, Decimal>(dec("1.0"), 1.0));
    assert!(!general_ne::<Decimal, i32, Decimal>(dec("1.0"), 1));
    assert!(!general_ne::<Decimal, f32, Decimal>(dec("1.0"), 1.0));
    assert!(!general_gt::<Decimal, i32, Decimal>(dec("1.0"), 2));
    assert!(!general_gt::<Decimal, f32, Decimal>(dec("1.0"), 2.0));
    assert!(general_le::<Decimal, i32, Decimal>(dec("1.0"), 2));
    assert!(general_le::<Decimal, f32, Decimal>(dec("1.0"), 2.1));
    assert!(!general_ge::<Decimal, i32, Decimal>(dec("1.0"), 2));
    assert!(!general_ge::<Decimal, f32, Decimal>(dec("1.0"), 2.1));
    assert!(general_lt::<Decimal, i32, Decimal>(dec("1.0"), 2));
    assert!(general_lt::<Decimal, f32, Decimal>(dec("1.0"), 2.1));
    assert!(general_is_distinct_from::<Decimal, i32, Decimal>(
        Some(dec("1.0")),
        Some(2)
    ));
    assert!(general_is_distinct_from::<Decimal, f32, Decimal>(
        Some(dec("1.0")),
        Some(2.0)
    ));
    assert!(general_is_distinct_from::<Decimal, f32, Decimal>(
        Some(dec("1.0")),
        None
    ));
    assert!(general_is_distinct_from::<Decimal, i32, Decimal>(
        None,
        Some(1)
    ));
    assert!(!general_is_distinct_from::<Decimal, i32, Decimal>(
        Some(dec("1.0")),
        Some(1)
    ));
    assert!(!general_is_distinct_from::<Decimal, f32, Decimal>(
        Some(dec("1.0")),
        Some(1.0)
    ));
    assert!(!general_is_distinct_from::<Decimal, f32, Decimal>(
        None, None
    ));
    assert!(general_eq::<OrderedF32, i32, OrderedF64>(1.0.into(), 1));
    assert!(!general_ne::<OrderedF32, i32, OrderedF64>(1.0.into(), 1));
    assert!(!general_lt::<OrderedF32, i32, OrderedF64>(1.0.into(), 1));
    assert!(general_le::<OrderedF32, i32, OrderedF64>(1.0.into(), 1));
    assert!(!general_gt::<OrderedF32, i32, OrderedF64>(1.0.into(), 1));
    assert!(general_ge::<OrderedF32, i32, OrderedF64>(1.0.into(), 1));
    assert!(!general_is_distinct_from::<OrderedF32, i32, OrderedF64>(
        Some(1.0.into()),
        Some(1)
    ));
    assert!(general_eq::<i64, i32, i64>(1i64, 1));
    assert!(!general_ne::<i64, i32, i64>(1i64, 1));
    assert!(!general_lt::<i64, i32, i64>(1i64, 1));
    assert!(general_le::<i64, i32, i64>(1i64, 1));
    assert!(!general_gt::<i64, i32, i64>(1i64, 1));
    assert!(general_ge::<i64, i32, i64>(1i64, 1));
    assert!(!general_is_distinct_from::<i64, i32, i64>(
        Some(1i64),
        Some(1)
    ));
}

#[test]
fn test_conjunction() {
    assert!(not(false));
    assert!(!and(Some(true), Some(false)).unwrap());
    assert!(or(Some(true), Some(false)).unwrap());
}
#[test]
fn test_cast() {
    assert_eq!(
        try_cast::<_, NaiveDateTimeWrapper>(NaiveDateWrapper::from_ymd_uncheck(1994, 1, 1))
            .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    )
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}
