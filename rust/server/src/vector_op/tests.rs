use crate::vector_op::arithmetic_op::*;
use crate::vector_op::cmp::*;
use rust_decimal::Decimal;
use std::str::FromStr;
#[test]
fn test_arithmetic() {
    assert_eq!(
        deci_add::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap(),
        Decimal::from_str("2.0").unwrap()
    );
    assert_eq!(
        deci_sub::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap(),
        Decimal::from_str("-1.0").unwrap()
    );
    assert_eq!(
        deci_mul::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap(),
        Decimal::from_str("2.0").unwrap()
    );
    assert_eq!(
        deci_div::<Decimal, i32, Decimal>(Decimal::from_str("2.0").unwrap(), 2).unwrap(),
        Decimal::from_str("1.0").unwrap()
    );
    assert_eq!(
        deci_mod::<Decimal, i32, Decimal>(Decimal::from_str("2.0").unwrap(), 2).unwrap(),
        Decimal::from_str("0").unwrap()
    );
    assert_eq!(int_add::<i16, i32, i32>(1i16, 1i32).unwrap(), 2i32);
    assert_eq!(int_sub::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(int_mul::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(int_div::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(prim_mod::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);

    assert_eq!(
        deci_f_add::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), -1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        deci_f_sub::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        deci_f_div::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        deci_f_mul::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        deci_f_mod::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert!(float_add::<i32, f32, f32>(-1i32, 1f32).unwrap().abs() < f32::EPSILON);
    assert!(float_sub::<i32, f32, f32>(1i32, 1f32).unwrap().abs() < f32::EPSILON);
    assert!(float_mul::<i32, f32, f32>(0i32, 1f32).unwrap().abs() < f32::EPSILON);
    assert!(float_div::<i32, f32, f32>(0i32, 1f32).unwrap().abs() < f32::EPSILON);
}

#[test]
fn test_comparison() {
    assert!(deci_eq::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap());
    assert!(deci_f_eq::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1.0).unwrap());
    assert!(!deci_neq::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap());
    assert!(!deci_f_neq::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1.0).unwrap());
    assert!(!deci_gt::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(!deci_f_gt::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.0).unwrap());
    assert!(deci_leq::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(deci_f_leq::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(!deci_geq::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(!deci_f_geq::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(deci_lt::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(deci_f_lt::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(prim_eq::<f32, i32, f32>(1.0, 1).unwrap());
    assert!(!prim_neq::<f32, i32, f32>(1.0, 1).unwrap());
    assert!(!prim_lt::<f32, i32, f32>(1.0, 1).unwrap());
    assert!(prim_leq::<f32, i32, f32>(1.0, 1).unwrap());
    assert!(!prim_gt::<f32, i32, f32>(1.0, 1).unwrap());
    assert!(prim_geq::<f32, i32, f32>(1.0, 1).unwrap());
    assert!(prim_eq::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!prim_neq::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!prim_lt::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(prim_leq::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!prim_gt::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(prim_geq::<i64, i32, i64>(1i64, 1).unwrap());
}
