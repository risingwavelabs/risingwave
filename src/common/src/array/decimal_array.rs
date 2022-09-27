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

use std::hash::{Hash, Hasher};
use std::mem::size_of;

use itertools::Itertools;
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::Buffer;
use risingwave_pb::data::{Array as ProstArray, ArrayType};

use super::{Array, ArrayBuilder, ArrayIterator, NULL_VAL_FOR_HASH};
use crate::array::{ArrayBuilderImpl, ArrayMeta};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::types::Decimal;

#[derive(Debug, Clone)]
pub struct DecimalArray {
    bitmap: Bitmap,
    data: Vec<Decimal>,
}

impl DecimalArray {
    pub fn from_slice(data: &[Option<Decimal>]) -> Self {
        let mut builder = <Self as Array>::Builder::new(data.len());
        for i in data {
            builder.append(*i);
        }
        builder.finish()
    }
}

impl Array for DecimalArray {
    type Builder = DecimalArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = Decimal;
    type RefItem<'a> = Decimal;

    fn value_at(&self, idx: usize) -> Option<Decimal> {
        if !self.is_null(idx) {
            Some(self.data[idx])
        } else {
            None
        }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<Decimal> {
        if !self.is_null_unchecked(idx) {
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

    fn to_protobuf(&self) -> ProstArray {
        let mut offset_buffer = Vec::<u8>::with_capacity(self.data.len() * size_of::<usize>());
        let mut data_buffer = Vec::<u8>::new();
        let mut offset = 0usize;
        for (d, not_null) in self.data.iter().zip_eq(self.null_bitmap().iter()) {
            let s = d.to_string();
            let b = s.as_bytes();
            if not_null {
                offset_buffer.extend_from_slice(&offset.to_be_bytes());
                data_buffer.extend_from_slice(b);
                offset += b.len();
            }
        }
        offset_buffer.extend_from_slice(&offset.to_be_bytes());

        let values = vec![
            Buffer {
                compression: CompressionType::None as i32,
                body: offset_buffer,
            },
            Buffer {
                compression: CompressionType::None as i32,
                body: data_buffer,
            },
        ];
        let null_bitmap = self.null_bitmap().to_protobuf();
        ProstArray {
            null_bitmap: Some(null_bitmap),
            values,
            array_type: ArrayType::Decimal as i32,
            struct_array_data: None,
            list_array_data: None,
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.data[idx].normalize().hash(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        let array_builder = DecimalArrayBuilder::new(capacity);
        ArrayBuilderImpl::Decimal(array_builder)
    }
}

/// `DecimalArrayBuilder` constructs a `DecimalArray` from `Option<Decimal>`.
#[derive(Debug)]
pub struct DecimalArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<Decimal>,
}

impl ArrayBuilder for DecimalArrayBuilder {
    type ArrayType = DecimalArray;

    fn with_meta(capacity: usize, _meta: ArrayMeta) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn append(&mut self, value: Option<Decimal>) {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.append(false);
                self.data.push(Decimal::default());
            }
        }
    }

    fn append_array(&mut self, other: &DecimalArray) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn finish(self) -> DecimalArray {
        DecimalArray {
            bitmap: self.bitmap.finish(),
            data: self.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use itertools::Itertools;
    use num_traits::FromPrimitive;

    use super::*;

    #[test]
    fn test_decimal_builder() {
        let v = (0..1000).map(Decimal::from_i64).collect_vec();
        let mut builder = DecimalArrayBuilder::new(0);
        for i in &v {
            builder.append(*i);
        }
        let a = builder.finish();
        let res = v.iter().zip_eq(a.iter()).all(|(a, b)| *a == b);
        assert!(res);
    }

    #[test]
    fn test_decimal_array_to_protobuf() {
        let input = vec![
            Some(Decimal::from_str("1.01").unwrap()),
            Some(Decimal::from_str("2.02").unwrap()),
            None,
            Some(Decimal::from_str("4.04").unwrap()),
        ];

        let array = DecimalArray::from_slice(&input);
        let buffers = array.to_protobuf().values;

        assert_eq!(buffers.len(), 2);

        let (offset, mut offset_buffer) =
            input
                .iter()
                .fold((0usize, Vec::new()), |(o, mut v), d| match d {
                    Some(d) => {
                        v.extend_from_slice(&o.to_be_bytes());
                        (o + d.to_string().as_bytes().len(), v)
                    }
                    None => (o, v),
                });
        offset_buffer.extend_from_slice(&offset.to_be_bytes());

        let data_buffer = input.iter().fold(Vec::new(), |mut v, d| match d {
            Some(d) => {
                v.extend_from_slice(d.to_string().as_bytes());
                v
            }
            None => v,
        });

        assert_eq!(buffers[0].get_body(), &offset_buffer);
        assert_eq!(buffers[1].get_body(), &data_buffer);
    }

    #[test]
    fn test_decimal_array_hash() {
        use std::hash::BuildHasher;

        use twox_hash::RandomXxHashBuilder64;

        use super::super::test_util::{hash_finish, test_hash};

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
                    #[allow(clippy::approx_constant)]
                    1 => Decimal::from_f32(3.14),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 5 {
                    0 => Decimal::from_u32(0),
                    1 => Decimal::from_u8(123),
                    #[allow(clippy::approx_constant)]
                    2 => Decimal::from_f64(3.1415926),
                    #[allow(clippy::approx_constant)]
                    3 => Decimal::from_f32(3.14),
                    4 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs
            .iter()
            .map(|v| {
                let mut builder = DecimalArrayBuilder::new(0);
                for i in v {
                    builder.append(*i);
                }
                builder.finish()
            })
            .collect_vec();

        let hasher_builder = RandomXxHashBuilder64::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter().zip_eq(&mut states).for_each(|(x, state)| match x {
                Some(inner) => inner.hash(state),
                None => NULL_VAL_FOR_HASH.hash(state),
            })
        });
        let hashes = hash_finish(&mut states[..]);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 30);

        test_hash(arrs, hashes, hasher_builder);
    }
}
