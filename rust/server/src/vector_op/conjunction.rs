use crate::array::{Array, ArrayBuilder, BoolArray, BoolArrayBuilder};
use crate::error::Result;
use crate::types::Scalar;

#[inline(always)]
fn scalar_and(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(l), Some(r)) => Some(l && r),
        _ => None,
    }
}

#[inline(always)]
fn scalar_or(l: Option<bool>, r: Option<bool>) -> Option<bool> {
    match (l, r) {
        (Some(l), Some(r)) => Some(l || r),
        _ => None,
    }
}

#[inline(always)]
fn scalar_not(l: Option<bool>) -> Option<bool> {
    l.map(|l| !l)
}

pub fn vector_binary_op<'a, A1, A2, A3, F>(a: &'a A1, b: &'a A2, f: F) -> Result<A3>
where
    A1: Array + 'a,
    A2: Array + 'a,
    A3: Array + 'a,
    F: Fn(Option<A1::RefItem<'a>>, Option<A2::RefItem<'a>>) -> Option<A3::OwnedItem>,
{
    let mut builder = A3::Builder::new(a.len())?;
    for (a, b) in a.iter().zip(b.iter()) {
        if let Some(x) = f(a, b) {
            builder.append(Some(x.as_scalar_ref()))?;
        } else {
            builder.append(None)?;
        }
    }
    builder.finish()
}

pub(crate) fn vector_or(left: &BoolArray, right: &BoolArray) -> Result<BoolArray> {
    vector_binary_op(left, right, scalar_or)
}

pub(crate) fn vector_and(left: &BoolArray, right: &BoolArray) -> Result<BoolArray> {
    vector_binary_op(left, right, scalar_and)
}

pub(crate) fn vector_not(input: &BoolArray) -> Result<BoolArray> {
    let mut builder = BoolArrayBuilder::new(input.len())?;
    for value in input.iter() {
        builder.append(scalar_not(value))?;
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and() {
        let vec1 = vec![Some(true), Some(true), Some(false), Some(false)];
        let vec2 = vec![None, Some(false), Some(true), None];
        let left = BoolArray::from_slice(&vec1).unwrap();
        let right = BoolArray::from_slice(&vec2).unwrap();
        let res = vector_and(&left, &right).unwrap();

        res.iter()
            .enumerate()
            .all(|(idx, t)| scalar_and(vec1[idx], vec2[idx]) == t);
    }

    #[test]
    fn test_or() {
        let vec1 = vec![Some(true), Some(true), Some(false), Some(false)];
        let vec2 = vec![None, Some(false), Some(true), None];
        let left = BoolArray::from_slice(&vec1).unwrap();
        let right = BoolArray::from_slice(&vec2).unwrap();
        let res = vector_or(&left, &right).unwrap();

        res.iter()
            .enumerate()
            .all(|(idx, t)| scalar_or(vec1[idx], vec2[idx]) == t);
    }

    #[test]
    fn test_not() {
        let vec1 = vec![Some(true), Some(true), Some(false), Some(false)];
        let left = BoolArray::from_slice(&vec1).unwrap();
        let res = vector_not(&left).unwrap();

        res.iter()
            .enumerate()
            .all(|(idx, t)| scalar_not(vec1[idx]) == t);
    }
}
