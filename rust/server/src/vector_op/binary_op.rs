use crate::array::{Array, ArrayRef, BoolArray, PrimitiveArray};
use crate::error::Result;
use crate::types::PrimitiveDataType;
use crate::util::downcast_ref;

// Again, assuming vector binary ops are over arrays with the same type.
pub fn vec_binary_op<TA, IA, TB, IB, TR, F>(
    left: IA,
    right: IB,
    mut op: F,
) -> Result<Vec<Option<TR>>>
where
    IA: IntoIterator<Item = Option<TA>>,
    IB: IntoIterator<Item = Option<TB>>,
    F: FnMut(Option<TA>, Option<TB>) -> Result<Option<TR>>,
{
    left.into_iter()
        .zip(right)
        .map(|pair| op(pair.0, pair.1))
        .collect::<Result<Vec<Option<TR>>>>()
}

pub fn vec_cmp_primitive_array<T, F>(
    left_array: &dyn Array,
    right_array: &dyn Array,
    op: F,
) -> Result<ArrayRef>
where
    T: PrimitiveDataType,
    F: FnMut(Option<T::N>, Option<T::N>) -> Result<Option<bool>>,
{
    let left_array: &PrimitiveArray<T> = downcast_ref(left_array)?;
    let right_array: &PrimitiveArray<T> = downcast_ref(right_array)?;

    let ret = vec_binary_op(left_array.as_iter()?, right_array.as_iter()?, op)?;
    BoolArray::from_values(ret)
}

#[cfg(test)]
mod tests {
    use crate::array2::{Array, PrimitiveArray};
    use crate::error::Result;
    use crate::types::*;
    use crate::vector_op::add_op::{vector_add_primitive_float, vector_add_primitive_integer};
    use crate::vector_op::sub_op::vector_sub_primitive_integer;

    fn test_primitive_type<T1, T2, T3, F>(
        left: Vec<Option<T1>>,
        right: Vec<Option<T2>>,
        expected: Vec<Option<T3>>,
        func: F,
        should_error: bool,
    ) where
        T1: NativeType,
        T2: NativeType,
        T3: NativeType,
        F: Fn(PrimitiveArray<T1>, PrimitiveArray<T2>) -> Result<PrimitiveArray<T3>>,
    {
        let ret = func(
            PrimitiveArray::<T1>::from_slice(&left).unwrap(),
            PrimitiveArray::<T2>::from_slice(&right).unwrap(),
        );
        if should_error {
            assert!(ret.is_err());
            return;
        } else {
            let ret = ret.expect("should success");
            assert_eq!(expected, ret.iter().collect::<Vec<Option<T3>>>());
        }
    }

    // TODO(chi): refactor to vectorized test
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

        // test_primitive_type(
        //   vec![Some(5i32), None, Some(3i32)],
        //   vec![Some(9i32), Some(8i32), Some(-8i32)],
        //   vec![Some(45i32), None, Some(-24i32)],
        //   vector_mul_primitive_integer,
        // );

        // test_primitive_type(
        //   vec![Some(10i32), None, Some(9i32)],
        //   vec![Some(5i32), Some(8i32), Some(2i32)],
        //   vec![Some(2i32), None, Some(4i32)],
        //   vector_div_primitive_integer,
        // );

        test_primitive_type(
            vec![Some(3f32), None, Some(9f32)],
            vec![Some(5f32), Some(8f32), Some(2f32)],
            vec![Some(8f32), None, Some(11f32)],
            vector_add_primitive_float,
            false,
        );

        // test_primitive_type(
        //   vec![Some(10i32), None, Some(9i32)],
        //   vec![Some(5i32), Some(8i32), Some(2i32)],
        //   vec![Some(0i32), None, Some(1i32)],
        //   vector_mod_primitive_integer,
        // );
    }

    // #[test]
    // fn test_primitive_overflow() {
    //   assert!(add_i16_i16(Some(10i16), Some(32765i16)).is_err());
    //   assert!(sub_i16_i16(Some(32765i16), Some(-10000i16)).is_err());
    //   assert!(mul_i16_i16(Some(10000i16), Some(10000i16)).is_err());
    //   assert!(div_i16_i16(Some(10000i16), Some(0i16)).is_err());
    //   assert!(add_f32_f32(Some(10000f32), Some(f32::INFINITY)).is_err());
    // }
}
