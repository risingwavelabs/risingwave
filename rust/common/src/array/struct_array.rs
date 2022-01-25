use core::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use super::{Array, ArrayBuilder, ArrayIterator};
use crate::error::Result;

/// This is a naive implementation of struct array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct StructArrayBuilder {
    values: Vec<Option<StructValue>>,
}

impl ArrayBuilder for StructArrayBuilder {
    type ArrayType = StructArray;

    fn new(_capacity: usize) -> Result<Self> {
        Ok(Self { values: vec![] })
    }

    fn append(&mut self, value: Option<StructValue>) -> Result<()> {
        self.values.push(value);
        Ok(())
    }

    fn append_array(&mut self, _other: &StructArray) -> Result<()> {
        todo!()
    }

    fn finish(self) -> Result<StructArray> {
        Ok(StructArray {
            values: self.values,
        })
    }
}

#[derive(Debug)]
pub struct StructArray {
    values: Vec<Option<StructValue>>,
}

impl Array for StructArray {
    type RefItem<'a> = StructValue;

    type OwnedItem = StructValue;

    type Builder = StructArrayBuilder;

    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        *self.values.get(idx).unwrap()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        todo!()
    }

    fn to_protobuf(&self) -> Result<risingwave_pb::data::Array> {
        todo!()
    }

    fn null_bitmap(&self) -> &crate::buffer::Bitmap {
        todo!()
    }

    fn hash_at<H: std::hash::Hasher>(&self, _idx: usize, _state: &mut H) {
        todo!()
    }

    fn hash_vec<H: std::hash::Hasher>(&self, _hashers: &mut Vec<H>) {}

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn create_builder(&self, _capacity: usize) -> Result<super::ArrayBuilderImpl> {
        todo!()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialOrd, Ord, Default, Deserialize)]
pub struct StructValue {}

impl fmt::Display for StructValue {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl Hash for StructValue {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl Serialize for StructValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl PartialEq for StructValue {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
