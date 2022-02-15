use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime};

use crate::types::{
    Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, OrderedF32, OrderedF64,
};
use crate::vector_op::arithmetic_op::*;
use crate::vector_op::cast::date_to_timestamp;
use crate::vector_op::cmp::*;
use crate::vector_op::conjunction::*;
#[test]
fn test_arithmetic() {
    assert_eq!(
        general_add::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap(),
        Decimal::from_str("2.0").unwrap()
    );
    assert_eq!(
        general_sub::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap(),
        Decimal::from_str("-1.0").unwrap()
    );
    assert_eq!(
        general_mul::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap(),
        Decimal::from_str("2.0").unwrap()
    );
    assert_eq!(
        general_div::<Decimal, i32, Decimal>(Decimal::from_str("2.0").unwrap(), 2).unwrap(),
        Decimal::from_str("1.0").unwrap()
    );
    assert_eq!(
        general_mod::<Decimal, i32, Decimal>(Decimal::from_str("2.0").unwrap(), 2).unwrap(),
        Decimal::from_str("0").unwrap()
    );
    assert_eq!(
        general_neg::<Decimal>(Decimal::from_str("1.0").unwrap()).unwrap(),
        Decimal::from_str("-1.0").unwrap()
    );
    assert_eq!(general_add::<i16, i32, i32>(1i16, 1i32).unwrap(), 2i32);
    assert_eq!(general_sub::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(general_mul::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(general_div::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(general_mod::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(general_neg::<i16>(1i16).unwrap(), -1i16);

    assert_eq!(
        general_add::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), -1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_sub::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_div::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_mul::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_mod::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
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
        date_interval_add::<NaiveDateWrapper, IntervalUnit, NaiveDateTimeWrapper>(
            NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1)),
            IntervalUnit::from_month(12)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
    assert_eq!(
        interval_date_add::<IntervalUnit, NaiveDateWrapper, NaiveDateTimeWrapper>(
            IntervalUnit::from_month(12),
            NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1))
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
    assert_eq!(
        date_interval_sub::<NaiveDateWrapper, IntervalUnit, NaiveDateTimeWrapper>(
            NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1)),
            IntervalUnit::from_month(12)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1993-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_comparison() {
    assert!(general_eq::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap());
    assert!(general_eq::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1.0).unwrap());
    assert!(!general_ne::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap());
    assert!(!general_ne::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1.0).unwrap());
    assert!(!general_gt::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(!general_gt::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.0).unwrap());
    assert!(general_le::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(general_le::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(!general_ge::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(!general_ge::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(general_lt::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(general_lt::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(general_eq::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(!general_ne::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(!general_lt::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(general_le::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(!general_gt::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(general_ge::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(general_eq::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!general_ne::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!general_lt::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(general_le::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!general_gt::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(general_ge::<i64, i32, i64>(1i64, 1).unwrap());
}

#[test]
fn test_conjunction() {
    assert!(not(Some(false)).unwrap().unwrap());
    assert!(!and(Some(true), Some(false)).unwrap().unwrap());
    assert!(or(Some(true), Some(false)).unwrap().unwrap());
}
#[test]
fn test_cast() {
    assert_eq!(
        date_to_timestamp(NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1))).unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    )
}
