use std::hash::{Hash, Hasher};

use super::{Array, ArrayBuilder, ArrayIterator, NULL_VAL_FOR_HASH};
use crate::buffer::Bitmap;
use crate::error::Result;

use risingwave_proto::data::Buffer;
use rust_decimal::Decimal;

#[derive(Debug)]
pub struct DecimalArray {
    bitmap: Bitmap,
    data: Vec<Decimal>,
}

impl DecimalArray {
    pub fn from_slice(data: &[Option<Decimal>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl Array for DecimalArray {
    type Builder = DecimalArrayBuilder;
    type RefItem<'a> = Decimal;
    type OwnedItem = Decimal;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<Decimal> {
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

    fn to_protobuf(&self) -> Result<Vec<Buffer>> {
        todo!()
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

/// `DecimalArrayBuilder` constructs a `DecimalArray` from `Option<Decimal>`.
pub struct DecimalArrayBuilder {
    bitmap: Vec<bool>,
    data: Vec<Decimal>,
}

impl ArrayBuilder for DecimalArrayBuilder {
    type ArrayType = DecimalArray;

    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<Decimal>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.push(false);
                self.data.push(Decimal::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &DecimalArray) -> Result<()> {
        self.bitmap.extend(other.bitmap.iter());
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(self) -> Result<DecimalArray> {
        Ok(DecimalArray {
            bitmap: Bitmap::from_vec(self.bitmap)?,
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use num_traits::FromPrimitive;

    #[test]
    fn test_decimal_builder() {
        let v = (0..1000).map(Decimal::from_i64).collect_vec();
        let mut builder = DecimalArrayBuilder::new(0).unwrap();
        for i in &v {
            builder.append(*i).unwrap();
        }
        let a = builder.finish().unwrap();
        let res = v.iter().zip(a.iter()).all(|(a, b)| *a == b);
        assert_eq!(res, true);
    }

    #[test]
    fn test_decimal_array_hash() {
        use super::super::test_util::{hash_finish, test_hash};
        use std::hash::BuildHasher;
        use twox_hash::RandomXxHashBuilder64;

        const ARR_NUM: usize = 3;
        const ARR_LEN: usize = 270;
        let vecs: [Vec<Option<Decimal>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Decimal::from_u32(0),
                    1 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Decimal::from_u32(0),
                    1 => Decimal::from_f32(3.14),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 5 {
                    0 => Decimal::from_u32(0),
                    1 => Decimal::from_u8(123),
                    2 => Decimal::from_f64(3.1415926),
                    3 => Decimal::from_f32(3.14),
                    4 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs
            .iter()
            .map(|v| {
                let mut builder = DecimalArrayBuilder::new(0).unwrap();
                for i in v {
                    builder.append(*i).unwrap();
                }
                builder.finish().unwrap()
            })
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
        assert_eq!(count, 30);

        test_hash(arrs, hashes, hasher_builder);
    }
}
