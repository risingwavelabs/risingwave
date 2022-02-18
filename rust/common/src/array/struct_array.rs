use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};

use itertools::Itertools;
use risingwave_pb::data::{Array as ProstArray, ArrayType as ProstArrayType};

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayIterator, ArrayMeta, ArrayType,
    NULL_VAL_FOR_HASH,
};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;
use crate::types::{Datum, DatumRef, ScalarRefImpl};

/// This is a naive implementation of struct array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct StructArrayBuilder {
    bitmap: BitmapBuilder,
    children_array: Vec<ArrayBuilderImpl>,
    children_types: Vec<ArrayType>,
    len: usize,
}

impl ArrayBuilder for StructArrayBuilder {
    type ArrayType = StructArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Result<Self> {
        panic!("Must use new_with_meta.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Result<Self> {
        Self::new_with_meta(capacity, ArrayMeta::Struct { children: vec![] })
    }

    fn new_with_meta(capacity: usize, meta: ArrayMeta) -> Result<Self> {
        if let ArrayMeta::Struct { children } = meta {
            let children_array = children
                .iter()
                .map(|a| a.create_array_builder(capacity))
                .try_collect()?;
            Ok(Self {
                bitmap: BitmapBuilder::with_capacity(capacity),
                children_array,
                children_types: children,
                len: 0,
            })
        } else {
            panic!("must be ArrayMeta::Struct");
        }
    }

    fn append(&mut self, value: Option<StructRef<'_>>) -> Result<()> {
        match value {
            None => {
                self.bitmap.append(false);
                for child in &mut self.children_array {
                    child.append_datum_ref(None)?;
                }
            }
            Some(v) => {
                self.bitmap.append(true);
                let fields = v.fields_ref();
                assert_eq!(fields.len(), self.children_array.len());
                for (field_idx, f) in fields.into_iter().enumerate() {
                    self.children_array[field_idx].append_datum_ref(f)?;
                }
            }
        }
        self.len += 1;
        Ok(())
    }

    fn append_array(&mut self, other: &StructArray) -> Result<()> {
        self.bitmap.append_bitmap(&other.bitmap);
        self.children_array
            .iter_mut()
            .enumerate()
            .try_for_each(|(i, a)| a.append_array(&other.children[i]))?;
        self.len += other.len();
        Ok(())
    }

    fn finish(mut self) -> Result<StructArray> {
        let children = self
            .children_array
            .into_iter()
            .map(|b| b.finish())
            .try_collect()?;
        Ok(StructArray {
            bitmap: self.bitmap.finish(),
            children,
            children_types: self.children_types,
            len: self.len,
        })
    }
}

#[derive(Debug)]
pub struct StructArray {
    bitmap: Bitmap,
    children: Vec<ArrayImpl>,
    children_types: Vec<ArrayType>,
    len: usize,
}

impl Array for StructArray {
    type RefItem<'a> = StructRef<'a>;

    type OwnedItem = StructValue;

    type Builder = StructArrayBuilder;

    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<StructRef<'_>> {
        if !self.is_null(idx) {
            Some(StructRef::Indexed { arr: self, idx })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> Result<ProstArray> {
        let children_array = self
            .children
            .iter()
            .map(|a| a.to_protobuf())
            .collect::<Result<Vec<ProstArray>>>()?;
        Ok(ProstArray {
            array_type: ProstArrayType::Struct as i32,
            children_array,
            null_bitmap: Some(self.bitmap.to_protobuf()?),
            values: vec![],
        })
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn hash_at<H: std::hash::Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.children.iter().for_each(|a| a.hash_at(idx, state))
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> Result<super::ArrayBuilderImpl> {
        let array_builder = StructArrayBuilder::new_with_meta(
            capacity,
            ArrayMeta::Struct {
                children: self.children_types.clone(),
            },
        )?;
        Ok(ArrayBuilderImpl::Struct(array_builder))
    }

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::Struct {
            children: self.children_types.clone(),
        }
    }
}

impl StructArray {
    pub fn from_protobuf(array: &ProstArray) -> Result<ArrayImpl> {
        ensure!(
            array.get_values().is_empty(),
            "Must have no buffer in a struct array"
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
        let cardinality = bitmap.len();
        let children = array
            .children_array
            .iter()
            .map(|child| ArrayImpl::from_protobuf(child, cardinality))
            .collect::<Result<Vec<ArrayImpl>>>()?;
        let children_types = array
            .children_array
            .iter()
            .map(ArrayType::from_protobuf)
            .collect::<Result<Vec<ArrayType>>>()?;
        let arr = StructArray {
            bitmap,
            children,
            children_types,
            len: cardinality,
        };
        Ok(arr.into())
    }

    pub fn children_array_types(&self) -> &[ArrayType] {
        &self.children_types
    }

    #[cfg(test)]
    pub fn from_slices(null_bitmap: &[bool], children: Vec<ArrayImpl>) -> Result<StructArray> {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::try_from(null_bitmap.to_vec())?;
        let children_types = children
            .iter()
            .map(|a| {
                assert_eq!(a.len(), cardinality);
                ArrayType::from_array_impl(a)
            })
            .collect::<Result<Vec<ArrayType>>>()?;
        Ok(StructArray {
            bitmap,
            children_types,
            len: cardinality,
            children,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialOrd, Ord, Default, PartialEq, Hash)]
pub struct StructValue {
    fields: Vec<Datum>,
}

impl fmt::Display for StructValue {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl StructValue {
    pub fn new(fields: Vec<Datum>) -> Self {
        Self { fields }
    }
}

#[derive(Copy, Clone)]
pub enum StructRef<'a> {
    Indexed { arr: &'a StructArray, idx: usize },
    ValueRef { val: &'a StructValue },
}

impl<'a> StructRef<'a> {
    pub fn fields_ref(&self) -> Vec<DatumRef<'a>> {
        match self {
            StructRef::Indexed { arr, idx } => {
                arr.children.iter().map(|a| a.value_at(*idx)).collect()
            }
            StructRef::ValueRef { val } => val
                .fields
                .iter()
                .map(|d| d.as_ref().map(|s| s.as_scalar_ref_impl()))
                .collect::<Vec<DatumRef<'a>>>(),
        }
    }
}

impl Hash for StructRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            StructRef::Indexed { arr, idx } => arr.hash_at(*idx, state),
            StructRef::ValueRef { val } => val.hash(state),
        }
    }
}

impl PartialEq for StructRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.fields_ref().eq(&other.fields_ref())
    }
}

impl PartialOrd for StructRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.fields_ref().partial_cmp(&other.fields_ref())
    }
}

fn cmp_struct_field(l: &Option<ScalarRefImpl>, r: &Option<ScalarRefImpl>) -> Ordering {
    match (l, r) {
        // Comparability check was performed by frontend beforehand.
        (Some(sl), Some(sr)) => sl.partial_cmp(sr).unwrap(),
        // Nulls are larger than everything, (1, null) > (1, 2) for example.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Debug for StructRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StructRef::Indexed { arr, idx } => arr
                .children
                .iter()
                .try_for_each(|a| a.value_at(*idx).fmt(f)),
            StructRef::ValueRef { val } => write!(f, "{:?}", val),
        }
    }
}

impl Display for StructRef<'_> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Eq for StructRef<'_> {}

impl Ord for StructRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order between two structs is deterministic.
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array, try_match_expand};

    // Empty struct is allowed in postgres.
    // `CREATE TYPE foo_empty as ();`, e.g.
    #[test]
    fn test_struct_new_empty() {
        let arr = StructArray::from_slices(&[true, false, true, false], vec![]).unwrap();
        let actual = StructArray::from_protobuf(&arr.to_protobuf().unwrap()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);
    }

    #[test]
    fn test_struct_with_fields() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[false, true, false, true],
            vec![
                array! { I32Array, [None, Some(1), None, Some(2)] }.into(),
                array! { F32Array, [None, Some(3.0), None, Some(4.0)] }.into(),
            ],
        )
        .unwrap();
        let actual = StructArray::from_protobuf(&arr.to_protobuf().unwrap()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);

        let arr = try_match_expand!(actual, ArrayImpl::Struct).unwrap();
        let struct_values = arr
            .iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec();
        assert_eq!(
            struct_values,
            vec![
                None,
                Some(StructValue::new(vec![
                    Some(ScalarImpl::Int32(1)),
                    Some(ScalarImpl::Float32(3.0.into())),
                ])),
                None,
                Some(StructValue::new(vec![
                    Some(ScalarImpl::Int32(2)),
                    Some(ScalarImpl::Float32(4.0.into())),
                ])),
            ]
        );
    }
}
