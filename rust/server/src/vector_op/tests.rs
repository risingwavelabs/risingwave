use crate::array2::{Array, PrimitiveArray, PrimitiveArrayItemType};
use crate::error::Result;
use crate::vector_op::add_op::*;
use crate::vector_op::div_op::*;
use crate::vector_op::mod_op::*;
use crate::vector_op::mul_op::*;
use crate::vector_op::sub_op::*;

fn test_primitive_type<T1, T2, T3, F>(
    left: Vec<Option<T1>>,
    right: Vec<Option<T2>>,
    expected: Vec<Option<T3>>,
    func: F,
    should_error: bool,
) where
    T1: PrimitiveArrayItemType,
    T2: PrimitiveArrayItemType,
    T3: PrimitiveArrayItemType,
    F: Fn(&PrimitiveArray<T1>, &PrimitiveArray<T2>) -> Result<PrimitiveArray<T3>>,
{
    let ret = func(
        &PrimitiveArray::<T1>::from_slice(&left).unwrap(),
        &PrimitiveArray::<T2>::from_slice(&right).unwrap(),
    );
    if should_error {
        assert!(ret.is_err());
    } else {
        let ret = ret.expect("should success");
        assert_eq!(expected, ret.iter().collect::<Vec<Option<T3>>>());
    }
}

#[test]
fn test_primitive() {
    test_primitive_type(
        vec![Some(15i16), None, Some(2i16)],
        vec![Some(127i16), Some(3i16), None],
        vec![Some(142i16), None, None],
        vector_add_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(15i32), None, Some(2i32)],
        vec![Some(14i32), None, Some(7i32)],
        vec![Some(29i32), None, Some(9i32)],
        vector_add_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(15i64), None, Some(2i64)],
        vec![Some(19i64), None, Some(8i64)],
        vec![Some(34i64), None, Some(10i64)],
        vector_add_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(15i32), None, Some(2i32)],
        vec![Some(14i32), None, None],
        vec![Some(1i32), None, None],
        vector_sub_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(15i64), None, Some(2i64)],
        vec![Some(19i64), None, Some(8i64)],
        vec![Some(-4i64), None, Some(-6i64)],
        vector_sub_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(5i32), None, Some(3i32)],
        vec![Some(9i32), Some(8i32), Some(-8i32)],
        vec![Some(45i32), None, Some(-24i32)],
        vector_mul_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(10i32), None, Some(9i32)],
        vec![Some(5i32), Some(8i32), Some(2i32)],
        vec![Some(2i32), None, Some(4i32)],
        vector_div_primitive_integer,
        false,
    );

    test_primitive_type(
        vec![Some(3f32), None, Some(9f32)],
        vec![Some(5f32), Some(8f32), Some(2f32)],
        vec![Some(8f32), None, Some(11f32)],
        vector_add_primitive_float,
        false,
    );

    test_primitive_type(
        vec![Some(10i32), None, Some(9i32)],
        vec![Some(5i32), Some(8i32), Some(2i32)],
        vec![Some(0i32), None, Some(1i32)],
        vector_mod_primitive,
        false,
    );
}

#[test]
fn test_primitive_overflow() {
    test_primitive_type(
        vec![Some(10i16)],
        vec![Some(32765i16)],
        vec![Some(0i16)],
        vector_add_primitive_integer,
        true,
    );

    test_primitive_type(
        vec![Some(32765i16)],
        vec![Some(-10000i16)],
        vec![Some(0i16)],
        vector_sub_primitive_integer,
        true,
    );

    test_primitive_type(
        vec![Some(10000i16)],
        vec![Some(10000i16)],
        vec![Some(0i16)],
        vector_mul_primitive_integer,
        true,
    );

    test_primitive_type(
        vec![Some(10000i16)],
        vec![Some(0i16)],
        vec![Some(0i16)],
        vector_div_primitive_integer,
        true,
    );

    test_primitive_type(
        vec![Some(10000f32)],
        vec![Some(f32::INFINITY)],
        vec![Some(0f32)],
        vector_add_primitive_float,
        true,
    );
}
