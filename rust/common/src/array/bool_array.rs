use std::hash::{Hash, Hasher};

use super::{Array, ArrayBuilder, ArrayIterator, NULL_VAL_FOR_HASH};
use crate::buffer::Bitmap;
use crate::error::Result;
use risingwave_proto::data::{Buffer as BufferProto, Buffer_CompressionType};
use std::mem::size_of;

#[derive(Debug)]
pub struct BoolArray {
    bitmap: Bitmap,
    data: Vec<bool>,
}

impl BoolArray {
    pub fn from_slice(data: &[Option<bool>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl Array for BoolArray {
    type Builder = BoolArrayBuilder;
    type RefItem<'a> = bool;
    type OwnedItem = bool;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<bool> {
        if !self.is_null(idx) {
            Some(self.data[idx])
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> Result<Vec<BufferProto>> {
        let values = {
            let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<bool>());

            for v in self.iter().flatten() {
                let bool_numeric = if v { 1 } else { 0 } as u8;
                output_buffer.push(bool_numeric);
            }

            let mut b = BufferProto::new();
            b.set_compression(Buffer_CompressionType::NONE);
            b.set_body(output_buffer);
            b
        };
        Ok(vec![values])
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.data[idx].hash(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }
}

/// `BoolArrayBuilder` constructs a `BoolArray` from `Option<Bool>`.
#[derive(Debug)]
pub struct BoolArrayBuilder {
    bitmap: Vec<bool>,
    data: Vec<bool>,
}

impl ArrayBuilder for BoolArrayBuilder {
    type ArrayType = BoolArray;

    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<bool>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.push(false);
                self.data.push(bool::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &BoolArray) -> Result<()> {
        self.bitmap.extend(other.bitmap.iter());
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(self) -> Result<BoolArray> {
        Ok(BoolArray {
            bitmap: self.bitmap.try_into()?,
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    fn helper_test_builder(data: Vec<Option<bool>>) -> BoolArray {
        let mut builder = BoolArrayBuilder::new(data.len()).unwrap();
        for d in data {
            builder.append(d).unwrap();
        }
        builder.finish().unwrap()
    }

    #[test]
    fn test_bool_builder() {
        let v = (0..1000)
            .map(|x| {
                if x % 2 == 0 {
                    None
                } else if x % 3 == 0 {
                    Some(true)
                } else {
                    Some(false)
                }
            })
            .collect_vec();
        let array = helper_test_builder(v.clone());
        let res = v.iter().zip(array.iter()).all(|(a, b)| *a == b);
        assert!(res);
    }

    #[test]
    fn test_bool_array_hash() {
        use super::super::test_util::{hash_finish, test_hash};
        use std::hash::BuildHasher;
        use twox_hash::RandomXxHashBuilder64;

        const ARR_NUM: usize = 2;
        const ARR_LEN: usize = 48;
        let vecs: [Vec<Option<bool>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Some(true),
                    1 => Some(false),
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Some(true),
                    1 => Some(false),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs
            .iter()
            .map(|v| helper_test_builder(v.clone()))
            .collect_vec();

        let hasher_builder = RandomXxHashBuilder64::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter().zip(&mut states).for_each(|(x, state)| match x {
                Some(inner) => inner.hash(state),
                None => NULL_VAL_FOR_HASH.hash(state),
            })
        });
        let hashes = hash_finish(&mut states);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 6);

        test_hash(arrs, hashes, hasher_builder);
    }
}
