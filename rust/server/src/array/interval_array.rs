use std::hash::Hash;

use crate::array::{Array, ArrayBuilder, ArrayIterator};
use crate::buffer::Bitmap;
use crate::error::Result;
use crate::types::IntervalUnit;
use risingwave_proto::data::Buffer;

use super::NULL_VAL_FOR_HASH;

#[derive(Debug)]
pub struct IntervalArray {
    bitmap: Bitmap,
    months_buffer: Vec<i32>,
    days_buffer: Vec<i32>,
    // milliseconds
    ms_buffer: Vec<i64>,
}

pub struct IntervalArrayBuilder {
    bitmap: Vec<bool>,
    months_buffer: Vec<i32>,
    days_buffer: Vec<i32>,
    ms_buffer: Vec<i64>,
}

impl Array for IntervalArray {
    type Builder = IntervalArrayBuilder;
    type RefItem<'a> = IntervalUnit;
    type OwnedItem = IntervalUnit;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if !self.is_null(idx) {
            let months = self.months_buffer[idx];
            let days = self.days_buffer[idx];
            let ms = self.ms_buffer[idx];
            Some(IntervalUnit::new(months, days, ms))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.months_buffer.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> Result<Vec<Buffer>> {
        unimplemented!("To protobuf of Interval Array is not implemented for now")
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn hash_at<H: std::hash::Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.months_buffer[idx].hash(state);
            self.days_buffer[idx].hash(state);
            self.ms_buffer[idx].hash(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }
}

impl ArrayBuilder for IntervalArrayBuilder {
    type ArrayType = IntervalArray;
    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: Vec::with_capacity(capacity),
            months_buffer: Vec::with_capacity(capacity),
            days_buffer: Vec::with_capacity(capacity),
            ms_buffer: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<IntervalUnit>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.months_buffer.push(x.get_months());
                self.days_buffer.push(x.get_days());
                self.ms_buffer.push(x.get_ms());
            }

            None => {
                self.bitmap.push(false);
                self.months_buffer.push(0);
                self.days_buffer.push(0);
                self.ms_buffer.push(0);
            }
        };
        Ok(())
    }

    fn append_array(&mut self, other: &IntervalArray) -> Result<()> {
        self.bitmap.extend(other.bitmap.iter());
        self.months_buffer.extend_from_slice(&other.months_buffer);
        self.days_buffer.extend_from_slice(&other.days_buffer);
        self.ms_buffer.extend_from_slice(&other.ms_buffer);
        Ok(())
    }

    fn finish(self) -> Result<Self::ArrayType> {
        Ok(IntervalArray {
            bitmap: Bitmap::from_vec(self.bitmap)?,
            months_buffer: self.months_buffer,
            days_buffer: self.days_buffer,
            ms_buffer: self.ms_buffer,
        })
    }
}

#[cfg(test)]
mod tests {
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
    }
}
