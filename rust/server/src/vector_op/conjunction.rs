use crate::array::{ArrayRef, BoolArray};
use crate::error::Result;
use crate::util::downcast_ref;

use super::binary_op::vec_binary_op;

fn scalar_and(l: Option<bool>, r: Option<bool>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some(l & r)),
        _ => Ok(None),
    }
}

fn scalar_or(l: Option<bool>, r: Option<bool>) -> Result<Option<bool>> {
    match (l, r) {
        (Some(l), Some(r)) => Ok(Some(l | r)),
        _ => Ok(None),
    }
}

fn scalar_not(l: Option<bool>) -> Result<Option<bool>> {
    match l {
        Some(l) => Ok(Some(!l)),
        _ => Ok(None),
    }
}

pub(crate) fn vector_and(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    let res = vec_binary_op(
        (downcast_ref(left.as_ref())? as &BoolArray).as_iter()?,
        (downcast_ref(right.as_ref())? as &BoolArray).as_iter()?,
        scalar_and,
    )?;
    BoolArray::from_values(res)
}
pub(crate) fn vector_or(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    let res = vec_binary_op(
        (downcast_ref(left.as_ref())? as &BoolArray).as_iter()?,
        (downcast_ref(right.as_ref())? as &BoolArray).as_iter()?,
        scalar_or,
    )?;
    BoolArray::from_values(res)
}
pub(crate) fn vector_not(left: ArrayRef) -> Result<ArrayRef> {
    let left = (downcast_ref(left.as_ref())? as &BoolArray).as_iter()?;
    let res = left
        .map(scalar_not)
        .collect::<Result<Vec<Option<bool>>>>()?;
    BoolArray::from_values(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array::BoolArray, util::downcast_ref};

    #[test]
    fn test_and() {
        let vec1 = vec![Some(true), Some(true), Some(false), Some(false)];
        let vec2 = vec![None, Some(false), Some(true), None];
        let left = BoolArray::from_values(&vec1).unwrap();
        let right = BoolArray::from_values(&vec2).unwrap();
        let res = vector_and(left, right).unwrap();
        let res = downcast_ref(res.as_ref()).unwrap() as &BoolArray;

        res.as_iter()
            .unwrap()
            .enumerate()
            .all(|(idx, t)| scalar_and(vec1[idx], vec2[idx]).unwrap() == t);
    }

    #[test]
    fn test_or() {
        let vec1 = vec![Some(true), Some(true), Some(false), Some(false)];
        let vec2 = vec![None, Some(false), Some(true), None];
        let left = BoolArray::from_values(&vec1).unwrap();
        let right = BoolArray::from_values(&vec2).unwrap();
        let res = vector_and(left, right).unwrap();
        let res = downcast_ref(res.as_ref()).unwrap() as &BoolArray;
        res.as_iter()
            .unwrap()
            .enumerate()
            .all(|(idx, t)| scalar_or(vec1[idx], vec2[idx]).unwrap() == t);
    }

    #[test]
    fn test_not() {
        let vec1 = vec![Some(true), Some(true), Some(false), Some(false)];
        let left = BoolArray::from_values(&vec1).unwrap();
        let res = vector_not(left).unwrap();
        let res = downcast_ref(res.as_ref()).unwrap() as &BoolArray;

        res.as_iter()
            .unwrap()
            .enumerate()
            .all(|(idx, t)| scalar_not(vec1[idx]).unwrap() == t);
    }
}
