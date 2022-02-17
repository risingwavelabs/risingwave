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
use crate::types::{Datum, ScalarRefImpl};

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

    fn append(&mut self, _value: Option<StructRef<'_>>) -> Result<()> {
        todo!()
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
            Some(StructRef { arr: self, idx })
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
    pub fn from_protobuf(array: &ProstArray, cardinality: usize) -> Result<ArrayImpl> {
        ensure!(
            array.get_values().is_empty(),
            "Must have no buffer in a struct array"
        );
        let mut builder = StructArrayBuilder::new_with_meta(cardinality, ArrayMeta::Simple)?;
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
        let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
        let arr = StructArray {
            bitmap,
            children,
            children_types,
            len: cardinality,
        };
        builder.append_array(&arr)?;
        let arr = builder.finish()?;
        Ok(arr.into())
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
pub struct StructRef<'a> {
    arr: &'a StructArray,
    idx: usize,
}

impl<'a> StructRef<'a> {
    pub fn field_value(&self, field_idx: usize) -> Option<ScalarRefImpl> {
        self.arr.children[field_idx].value_at(self.idx)
    }

    pub fn fields_num(&self) -> usize {
        self.arr.children.len()
    }
}

impl Hash for StructRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.arr.hash_at(self.idx, state);
    }
}

impl PartialEq for StructRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        debug_assert!(
            self.arr.children_types == other.arr.children_types,
            "cannot compare dissimilar composite types"
        );
        if self.arr.children.len() != other.arr.children.len() {
            return false;
        }
        self.arr.children.iter().enumerate().all(|(col_idx, arr)| {
            let other_arr = &other.arr.children[col_idx];
            arr.value_at(self.idx) == other_arr.value_at(self.idx)
        })
    }
}

impl PartialOrd for StructRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        debug_assert!(
            self.arr.children_types == other.arr.children_types,
            "cannot compare dissimilar composite types"
        );
        for field_idx in 0..self.fields_num() {
            let ord = cmp_struct_field(&self.field_value(field_idx), &other.field_value(field_idx));
            if ord != Ordering::Equal {
                return Some(ord);
            }
        }
        Some(Ordering::Equal)
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
        self.arr
            .children
            .iter()
            .try_for_each(|a| a.value_at(self.idx).fmt(f))
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
