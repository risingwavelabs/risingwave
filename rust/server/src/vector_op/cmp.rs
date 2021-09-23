use num_traits::AsPrimitive;

use crate::array2::ArrayImpl::{Float32, Float64, Int16, Int32, Int64};
use crate::array2::{
    Array, ArrayBuilder, ArrayImpl, BoolArray, BoolArrayBuilder, PrimitiveArray,
    PrimitiveArrayItemType,
};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;

pub fn vec_cmp_primitive<T1, T2, T3, F>(
    left: &PrimitiveArray<T1>,
    right: &PrimitiveArray<T2>,
    mut op: F,
) -> Result<BoolArray>
where
    T1: PrimitiveArrayItemType + AsPrimitive<T3>,
    T2: PrimitiveArrayItemType + AsPrimitive<T3>,
    T3: PrimitiveArrayItemType,
    F: FnMut(T3, T3) -> bool,
{
    ensure!(left.len() == right.len());
    let mut builder = BoolArrayBuilder::new(left.len())?;
    for (l, r) in left.iter().zip(right.iter()) {
        let item = match (l, r) {
            (Some(l), Some(r)) => Some(op(l.as_(), r.as_())),
            _ => None,
        };
        builder.append(item)?;
    }
    builder.finish()
}

macro_rules! vec_cmp {
    ($lhs:expr, $rhs:expr, $f:ident) => {
        match ($lhs, $rhs) {
            // integer
            (Int16(l), Int16(r)) => vec_cmp_primitive::<i16, i16, i16, _>(l, r, $f::<i16>),
            (Int16(l), Int32(r)) => vec_cmp_primitive::<i16, i32, i32, _>(l, r, $f::<i32>),
            (Int16(l), Int64(r)) => vec_cmp_primitive::<i16, i64, i64, _>(l, r, $f::<i64>),
            (Int32(l), Int16(r)) => vec_cmp_primitive::<i32, i16, i32, _>(l, r, $f::<i32>),
            (Int32(l), Int32(r)) => vec_cmp_primitive::<i32, i32, i32, _>(l, r, $f::<i32>),
            (Int32(l), Int64(r)) => vec_cmp_primitive::<i32, i64, i64, _>(l, r, $f::<i64>),
            (Int64(l), Int16(r)) => vec_cmp_primitive::<i64, i16, i64, _>(l, r, $f::<i64>),
            (Int64(l), Int32(r)) => vec_cmp_primitive::<i64, i32, i64, _>(l, r, $f::<i64>),
            (Int64(l), Int64(r)) => vec_cmp_primitive::<i64, i64, i64, _>(l, r, $f::<i64>),
            // float
            (Float32(l), Float32(r)) => vec_cmp_primitive::<f32, f32, f32, _>(l, r, $f::<f32>),
            (Float32(l), Float64(r)) => vec_cmp_primitive::<f32, f64, f64, _>(l, r, $f::<f64>),
            (Float64(l), Float32(r)) => vec_cmp_primitive::<f64, f32, f64, _>(l, r, $f::<f64>),
            (Float64(l), Float64(r)) => vec_cmp_primitive::<f64, f64, f64, _>(l, r, $f::<f64>),
            _ => Err(InternalError("Unsupported proto type".to_string()).into()),
        }
    };
}

#[inline(always)]
fn scalar_eq<T: PartialEq>(l: T, r: T) -> bool {
    l == r
}

#[inline(always)]
fn scalar_neq<T: PartialEq>(l: T, r: T) -> bool {
    l != r
}

#[inline(always)]
fn scalar_gt<T: PartialEq + PartialOrd>(l: T, r: T) -> bool {
    l > r
}

#[inline(always)]
fn scalar_geq<T: PartialEq + PartialOrd>(l: T, r: T) -> bool {
    l >= r
}

#[inline(always)]
fn scalar_lt<T: PartialEq + PartialOrd>(l: T, r: T) -> bool {
    l < r
}

#[inline(always)]
fn scalar_leq<T: PartialEq + PartialOrd>(l: T, r: T) -> bool {
    l <= r
}

pub fn vec_eq(lhs: &ArrayImpl, rhs: &ArrayImpl) -> Result<BoolArray> {
    vec_cmp!(lhs, rhs, scalar_eq)
}

pub fn vec_neq(lhs: &ArrayImpl, rhs: &ArrayImpl) -> Result<BoolArray> {
    vec_cmp!(lhs, rhs, scalar_neq)
}

pub fn vec_lt(lhs: &ArrayImpl, rhs: &ArrayImpl) -> Result<BoolArray> {
    vec_cmp!(lhs, rhs, scalar_lt)
}

pub fn vec_leq(lhs: &ArrayImpl, rhs: &ArrayImpl) -> Result<BoolArray> {
    vec_cmp!(lhs, rhs, scalar_leq)
}

pub fn vec_gt(lhs: &ArrayImpl, rhs: &ArrayImpl) -> Result<BoolArray> {
    vec_cmp!(lhs, rhs, scalar_gt)
}

pub fn vec_geq(lhs: &ArrayImpl, rhs: &ArrayImpl) -> Result<BoolArray> {
    vec_cmp!(lhs, rhs, scalar_geq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    #[test]
    fn test_cmp() {
        mock_execute::<i32, i64, i64, _, _>(
            [Some(1), Some(3), None].to_vec(),
            [Some(1i64), Some(2i64), Some(3i64)].to_vec(),
            scalar_eq,
            vec_eq,
        );
        mock_execute::<i32, i64, i64, _, _>(
            [Some(1), Some(3), None].to_vec(),
            [Some(1i64), Some(2i64), Some(3i64)].to_vec(),
            scalar_lt,
            vec_lt,
        );

        mock_execute::<i32, i64, i64, _, _>(
            [Some(1), Some(3), None].to_vec(),
            [Some(1i64), Some(2i64), Some(3i64)].to_vec(),
            scalar_leq,
            vec_leq,
        );
        mock_execute::<f32, f64, f64, _, _>(
            [Some(1f32), Some(3f32), None].to_vec(),
            [Some(1f64), Some(2f64), Some(3f64)].to_vec(),
            scalar_neq,
            vec_neq,
        );
        mock_execute::<f32, f64, f64, _, _>(
            [Some(1f32), Some(3f32), None].to_vec(),
            [Some(1f64), Some(2f64), Some(3f64)].to_vec(),
            scalar_gt,
            vec_gt,
        );
        mock_execute::<f32, f64, f64, _, _>(
            [Some(1f32), Some(3f32), None].to_vec(),
            [Some(1f64), Some(2f64), Some(3f64)].to_vec(),
            scalar_geq,
            vec_geq,
        );
    }

    fn mock_execute<T1, T2, T3, F1, F2>(
        v1: Vec<Option<T1>>,
        v2: Vec<Option<T2>>,
        mut scalar_cmp: F1,
        mut vec_cmp: F2,
    ) where
        T1: PrimitiveArrayItemType + AsPrimitive<T3>,
        T2: PrimitiveArrayItemType + AsPrimitive<T3>,
        T3: PrimitiveArrayItemType,
        F1: FnMut(T3, T3) -> bool,
        F2: FnMut(&ArrayImpl, &ArrayImpl) -> Result<BoolArray>,
    {
        let lhs = PrimitiveArray::from_slice(&v1).unwrap();
        let rhs = PrimitiveArray::from_slice(&v2).unwrap();
        let sel = vec_cmp(&(lhs.into()), &(rhs.into())).unwrap();
        let res = sel.iter().enumerate().all(|(idx, s)| {
            s == {
                if let (Some(l), Some(r)) = (v1[idx], v2[idx]) {
                    Some(scalar_cmp(l.as_(), r.as_()))
                } else {
                    None
                }
            }
        });
        assert!(res);
    }
}
