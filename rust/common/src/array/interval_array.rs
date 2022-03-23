// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::Hash;

use risingwave_pb::data::Array as ProstArray;

use super::{ArrayMeta, NULL_VAL_FOR_HASH};
use crate::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayIterator};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;
use crate::types::interval::IntervalUnit;

#[derive(Debug)]
pub struct IntervalArray {
    bitmap: Bitmap,
    interval_buffer: Vec<IntervalUnit>,
}

#[derive(Debug)]
pub struct IntervalArrayBuilder {
    bitmap: BitmapBuilder,
    interval_buffer: Vec<IntervalUnit>,
}

impl IntervalArray {
    pub fn from_slice(data: &[Option<IntervalUnit>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl Array for IntervalArray {
    type Builder = IntervalArrayBuilder;
    type RefItem<'a> = IntervalUnit;
    type OwnedItem = IntervalUnit;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if !self.is_null(idx) {
            Some(self.interval_buffer[idx])
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.interval_buffer.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> ProstArray {
        unimplemented!("To protobuf of Interval Array is not implemented for now")
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn hash_at<H: std::hash::Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.interval_buffer[idx].hash(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        let array_builder = IntervalArrayBuilder::new(capacity)?;
        Ok(ArrayBuilderImpl::Interval(array_builder))
    }
}

impl ArrayBuilder for IntervalArrayBuilder {
    type ArrayType = IntervalArray;

    fn new_with_meta(capacity: usize, _meta: ArrayMeta) -> Result<Self> {
        Ok(Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            interval_buffer: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<IntervalUnit>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.interval_buffer.push(x);
            }

            None => {
                self.bitmap.append(false);
                self.interval_buffer.push(IntervalUnit::default());
            }
        };
        Ok(())
    }

    fn append_array(&mut self, other: &IntervalArray) -> Result<()> {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.interval_buffer
            .extend_from_slice(&other.interval_buffer);
        Ok(())
    }

    fn finish(mut self) -> Result<Self::ArrayType> {
        Ok(IntervalArray {
            bitmap: self.bitmap.finish(),
            interval_buffer: self.interval_buffer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::IntervalArray;
    use crate::array::interval_array::{IntervalArrayBuilder, IntervalUnit};
    use crate::array::{Array, ArrayBuilder};

    #[test]
    fn test_interval_array() {
        let cardinality = 5;
        let mut array_builder = IntervalArrayBuilder::new(cardinality).unwrap();
        for _ in 0..cardinality {
            let v = IntervalUnit::from_ymd(1, 0, 0);
            array_builder.append(Some(v)).unwrap();
        }
        let ret_arr = array_builder.finish().unwrap();
        for v in ret_arr.iter().flatten() {
            assert_eq!(v.get_years(), 1);
            assert_eq!(v.get_months(), 12);
            assert_eq!(v.get_days(), 0);
        }
        let ret_arr =
            IntervalArray::from_slice(&[Some(IntervalUnit::from_ymd(1, 0, 0)), None]).unwrap();
        let v = ret_arr.value_at(0).unwrap();
        assert_eq!(v.get_years(), 1);
        assert_eq!(v.get_months(), 12);
        assert_eq!(v.get_days(), 0);
        let v = ret_arr.value_at(1);
        assert_eq!(v, None);
    }
}
