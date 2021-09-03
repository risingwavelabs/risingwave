use super::binary_op::vec_cmp_primitive_array;
use crate::array::ArrayRef;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::types::{DataTypeKind, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};

macro_rules! vec_cmp {
    ($lhs:expr, $rhs:expr, $cmp:ident) => {
        match $lhs.data_type().data_type_kind() {
            DataTypeKind::Int16 => vec_cmp_primitive_array::<Int16Type, _>($lhs, $rhs, $cmp::<i16>),
            DataTypeKind::Int32 => vec_cmp_primitive_array::<Int32Type, _>($lhs, $rhs, $cmp::<i32>),
            DataTypeKind::Int64 => vec_cmp_primitive_array::<Int64Type, _>($lhs, $rhs, $cmp::<i64>),
            DataTypeKind::Float32 => {
                vec_cmp_primitive_array::<Float32Type, _>($lhs, $rhs, $cmp::<f32>)
            }
            DataTypeKind::Float64 => {
                vec_cmp_primitive_array::<Float64Type, _>($lhs, $rhs, $cmp::<f64>)
            }
            _ => Err(InternalError("Unsupported proto type".to_string()).into()),
        }
    };
}

#[inline(always)]
fn scalar_eq<T: PartialEq>(l: Option<T>, r: Option<T>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some((l) == (r))),
        _ => Ok(None),
    }
}

#[inline(always)]
fn scalar_neq<T: PartialEq>(l: Option<T>, r: Option<T>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some((l) != (r))),
        _ => Ok(None),
    }
}

#[inline(always)]
fn scalar_gt<T: PartialEq + PartialOrd>(l: Option<T>, r: Option<T>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some((l) > (r))),
        _ => Ok(None),
    }
}

#[inline(always)]
fn scalar_geq<T: PartialEq + PartialOrd>(l: Option<T>, r: Option<T>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some((l) >= (r))),
        _ => Ok(None),
    }
}

#[inline(always)]
fn scalar_lt<T: PartialEq + PartialOrd>(l: Option<T>, r: Option<T>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some((l) < (r))),
        _ => Ok(None),
    }
}

#[inline(always)]
fn scalar_leq<T: PartialEq + PartialOrd>(l: Option<T>, r: Option<T>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some((l) <= (r))),
        _ => Ok(None),
    }
}

pub(crate) fn vec_eq(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    ensure!(lhs.len() == rhs.len());
    vec_cmp!(lhs.as_ref(), rhs.as_ref(), scalar_eq)
}

pub(crate) fn vec_neq(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    ensure!(lhs.len() == rhs.len());
    vec_cmp!(lhs.as_ref(), rhs.as_ref(), scalar_neq)
}

pub(crate) fn vec_lt(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    ensure!(lhs.len() == rhs.len());
    vec_cmp!(lhs.as_ref(), rhs.as_ref(), scalar_lt)
}

pub(crate) fn vec_leq(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    ensure!(lhs.len() == rhs.len());
    vec_cmp!(lhs.as_ref(), rhs.as_ref(), scalar_leq)
}

pub(crate) fn vec_gt(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    ensure!(lhs.len() == rhs.len());
    vec_cmp!(lhs.as_ref(), rhs.as_ref(), scalar_gt)
}

pub(crate) fn vec_geq(lhs: ArrayRef, rhs: ArrayRef) -> Result<ArrayRef> {
    ensure!(lhs.len() == rhs.len());
    vec_cmp!(lhs.as_ref(), rhs.as_ref(), scalar_geq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{ArrayRef, BoolArray, PrimitiveArray};
    use crate::error::Result;
    use crate::types::{Int32Type, PrimitiveDataType};
    use crate::util::downcast_ref;
    #[test]
    fn test_cmp() {
        mock_execute::<Int32Type, _, _>(
            [Some(1), Some(3), None].to_vec(),
            [Some(1), Some(2), Some(3)].to_vec(),
            scalar_eq,
            vec_eq,
        );
        mock_execute::<Float32Type, _, _>(
            [Some(1.1), Some(3.1), None].to_vec(),
            [Some(1.2), Some(2.4), Some(3.0)].to_vec(),
            scalar_neq,
            vec_neq,
        );
        mock_execute::<Int32Type, _, _>(
            [Some(1), Some(3), None].to_vec(),
            [Some(1), Some(2), Some(3)].to_vec(),
            scalar_lt,
            vec_lt,
        );
        mock_execute::<Float32Type, _, _>(
            [Some(1.1), Some(3.1), None].to_vec(),
            [Some(1.2), Some(2.4), Some(3.0)].to_vec(),
            scalar_geq,
            vec_geq,
        );
    }

    fn mock_execute<T, F1, F2>(
        v1: Vec<Option<T::N>>,
        v2: Vec<Option<T::N>>,
        mut scalar_cmp: F1,
        mut vec_cmp: F2,
    ) where
        T: PrimitiveDataType,
        F1: FnMut(Option<T::N>, Option<T::N>) -> Result<Option<bool>>,
        F2: FnMut(ArrayRef, ArrayRef) -> Result<ArrayRef>,
    {
        let lhs = PrimitiveArray::<T>::from_values(v1.clone()).unwrap();
        let rhs = PrimitiveArray::<T>::from_values(v2.clone()).unwrap();
        let sel = vec_cmp(lhs, rhs).unwrap();
        let sel = downcast_ref(sel.as_ref()).unwrap() as &BoolArray;
        let sel_iter = sel.as_iter().unwrap();
        let res = sel_iter
            .enumerate()
            .all(|(idx, target)| target == scalar_cmp(v1[idx], v2[idx]).unwrap());
        assert_eq!(res, true);
    }
}
