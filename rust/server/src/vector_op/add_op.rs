use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::Result;
use crate::error::RwError;
use std::option::Option;

macro_rules! add_primitive_types {
    ($fn_name: ident, $left: ty, $right: ty) => {
        pub(crate) fn $fn_name(v1: Option<$left>, v2: Option<$right>) -> Result<Option<$right>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => match l2.checked_add(l1 as $right) {
                    Some(v) => Ok(Some(v)),
                    None => Err(RwError::from(NumericValueOutOfRange)),
                },
                _ => Ok(None),
            }
        }
    };
}

add_primitive_types!(add_i16_i16, i16, i16);
add_primitive_types!(add_i16_i32, i16, i32);
add_primitive_types!(add_i16_i64, i16, i64);
add_primitive_types!(add_i32_i32, i32, i32);
add_primitive_types!(add_i32_i64, i32, i64);
add_primitive_types!(add_i64_i64, i64, i64);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::PrimitiveArray;
    use crate::error::Result;
    use crate::types::*;
    use crate::util::downcast_ref;
    use crate::vector_op::binary_op::vec_binary_op_primitive_array;

    fn test_add_primitive_type<A, B, F>(
        left: Vec<Option<A::N>>,
        right: Vec<Option<B::N>>,
        expected: Vec<Option<B::N>>,
        op: F,
    ) where
        A: PrimitiveDataType,
        B: PrimitiveDataType,
        F: FnMut(Option<A::N>, Option<B::N>) -> Result<Option<B::N>>,
    {
        let left_array =
            PrimitiveArray::<A>::from_values(left).expect("Failed to build left values!");
        let right_array =
            PrimitiveArray::<B>::from_values(right).expect("Failed to build right values!");

        let ret = vec_binary_op_primitive_array::<A, B, B, F>(
            left_array.as_ref(),
            right_array.as_ref(),
            op,
        )
        .expect("Failed to get result");

        let ret: &PrimitiveArray<B> = downcast_ref(ret.as_ref()).expect("Failed to downcast");

        assert_eq!(
            expected,
            ret.as_iter()
                .expect("Failed to create iterator!")
                .collect::<Vec<Option<B::N>>>()
        );
    }

    #[test]
    fn test_add_primitive() {
        test_add_primitive_type::<Int16Type, Int16Type, _>(
            vec![Some(15i16), None, Some(2i16)],
            vec![Some(127i16), Some(3i16), None],
            vec![Some(142i16), None, None],
            add_i16_i16,
        );

        test_add_primitive_type::<Int16Type, Int32Type, _>(
            vec![Some(15i16), None, Some(2i16)],
            vec![Some(14i32), None, Some(7i32)],
            vec![Some(29i32), None, Some(9i32)],
            add_i16_i32,
        );

        test_add_primitive_type::<Int16Type, Int64Type, _>(
            vec![Some(15i16), None, Some(2i16)],
            vec![Some(19i64), None, Some(8i64)],
            vec![Some(34i64), None, Some(10i64)],
            add_i16_i64,
        );

        test_add_primitive_type::<Int32Type, Int32Type, _>(
            vec![Some(15i32), None, Some(2i32)],
            vec![Some(14i32), None, None],
            vec![Some(29i32), None, None],
            add_i32_i32,
        );

        test_add_primitive_type::<Int32Type, Int64Type, _>(
            vec![Some(15i32), None, Some(2i32)],
            vec![Some(19i64), None, Some(8i64)],
            vec![Some(34i64), None, Some(10i64)],
            add_i32_i64,
        );

        test_add_primitive_type::<Int64Type, Int64Type, _>(
            vec![Some(75i64), None, Some(19i64)],
            vec![Some(19i64), Some(8i64), Some(8i64)],
            vec![Some(94i64), None, Some(27i64)],
            add_i64_i64,
        );
    }

    #[test]
    fn test_add_primitive_overflow() {
        assert!(add_i16_i16(Some(10i16), Some(32765i16)).is_err());
    }
}
