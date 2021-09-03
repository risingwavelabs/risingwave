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

pub fn vec_binary_op_primitive_array<TA, TB, TR, F>(
    left_array: &dyn Array,
    right_array: &dyn Array,
    op: F,
) -> Result<ArrayRef>
where
    TA: PrimitiveDataType,
    TB: PrimitiveDataType,
    TR: PrimitiveDataType,
    F: FnMut(Option<TA::N>, Option<TB::N>) -> Result<Option<TR::N>>,
{
    let left_primitive_array: &PrimitiveArray<TA> = downcast_ref(left_array)?;
    let right_primitive_array: &PrimitiveArray<TB> = downcast_ref(right_array)?;

    let ret = vec_binary_op(
        left_primitive_array.as_iter()?,
        right_primitive_array.as_iter()?,
        op,
    )?;
    PrimitiveArray::<TR>::from_values(ret)
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
    use crate::array::PrimitiveArray;
    use crate::error::Result;
    use crate::types::*;
    use crate::util::downcast_ref;
    use crate::vector_op::add_op::*;
    use crate::vector_op::binary_op::vec_binary_op_primitive_array;
    use crate::vector_op::div_op::*;
    use crate::vector_op::mod_op::*;
    use crate::vector_op::mul_op::*;
    use crate::vector_op::sub_op::*;

    fn test_primitive_type<T, F>(
        left: Vec<Option<T::N>>,
        right: Vec<Option<T::N>>,
        expected: Vec<Option<T::N>>,
        op: F,
    ) where
        T: PrimitiveDataType,
        F: FnMut(Option<T::N>, Option<T::N>) -> Result<Option<T::N>>,
    {
        let left_array =
            PrimitiveArray::<T>::from_values(left).expect("Failed to build left values!");
        let right_array =
            PrimitiveArray::<T>::from_values(right).expect("Failed to build right values!");

        let ret = vec_binary_op_primitive_array::<T, T, T, F>(
            left_array.as_ref(),
            right_array.as_ref(),
            op,
        )
        .expect("Failed to get result");

        let ret: &PrimitiveArray<T> = downcast_ref(ret.as_ref()).expect("Failed to downcast");

        assert_eq!(
            expected,
            ret.as_iter()
                .expect("Failed to create iterator!")
                .collect::<Vec<Option<T::N>>>()
        );
    }

    #[test]
    fn test_primitive() {
        test_primitive_type::<Int16Type, _>(
            vec![Some(15i16), None, Some(2i16)],
            vec![Some(127i16), Some(3i16), None],
            vec![Some(142i16), None, None],
            add_i16_i16,
        );

        test_primitive_type::<Int32Type, _>(
            vec![Some(15i32), None, Some(2i32)],
            vec![Some(14i32), None, Some(7i32)],
            vec![Some(29i32), None, Some(9i32)],
            add_i32_i32,
        );

        test_primitive_type::<Int64Type, _>(
            vec![Some(15i64), None, Some(2i64)],
            vec![Some(19i64), None, Some(8i64)],
            vec![Some(34i64), None, Some(10i64)],
            add_i64_i64,
        );

        test_primitive_type::<Int32Type, _>(
            vec![Some(15i32), None, Some(2i32)],
            vec![Some(14i32), None, None],
            vec![Some(1i32), None, None],
            sub_i32_i32,
        );

        test_primitive_type::<Int64Type, _>(
            vec![Some(15i64), None, Some(2i64)],
            vec![Some(19i64), None, Some(8i64)],
            vec![Some(-4i64), None, Some(-6i64)],
            sub_i64_i64,
        );

        test_primitive_type::<Int32Type, _>(
            vec![Some(5i32), None, Some(3i32)],
            vec![Some(9i32), Some(8i32), Some(-8i32)],
            vec![Some(45i32), None, Some(-24i32)],
            mul_i32_i32,
        );

        test_primitive_type::<Int32Type, _>(
            vec![Some(10i32), None, Some(9i32)],
            vec![Some(5i32), Some(8i32), Some(2i32)],
            vec![Some(2i32), None, Some(4i32)],
            div_i32_i32,
        );

        test_primitive_type::<Float32Type, _>(
            vec![Some(3f32), None, Some(9f32)],
            vec![Some(5f32), Some(8f32), Some(2f32)],
            vec![Some(8f32), None, Some(11f32)],
            add_f32_f32,
        );

        test_primitive_type::<Int32Type, _>(
            vec![Some(10i32), None, Some(9i32)],
            vec![Some(5i32), Some(8i32), Some(2i32)],
            vec![Some(0i32), None, Some(1i32)],
            mod_i32_i32,
        );
    }

    #[test]
    fn test_primitive_overflow() {
        assert!(add_i16_i16(Some(10i16), Some(32765i16)).is_err());
        assert!(sub_i16_i16(Some(32765i16), Some(-10000i16)).is_err());
        assert!(mul_i16_i16(Some(10000i16), Some(10000i16)).is_err());
        assert!(div_i16_i16(Some(10000i16), Some(0i16)).is_err());
        assert!(add_f32_f32(Some(10000f32), Some(f32::INFINITY)).is_err());
    }
}
