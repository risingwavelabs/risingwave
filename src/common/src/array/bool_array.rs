// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::data::{ArrayType, PbArray};

use super::{Array, ArrayBuilder, ArrayMeta};
use crate::array::ArrayBuilderImpl;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoolArray {
    bitmap: Bitmap,
    data: Bitmap,
}

impl BoolArray {
    pub fn new(data: Bitmap, bitmap: Bitmap) -> Self {
        assert_eq!(bitmap.len(), data.len());
        Self { bitmap, data }
    }

    pub fn data(&self) -> &Bitmap {
        &self.data
    }

    pub fn to_bitmap(&self) -> Bitmap {
        &self.data & self.null_bitmap()
    }
}

impl FromIterator<Option<bool>> for BoolArray {
    fn from_iter<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl<'a> FromIterator<&'a Option<bool>> for BoolArray {
    fn from_iter<I: IntoIterator<Item = &'a Option<bool>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl FromIterator<bool> for BoolArray {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let data: Bitmap = iter.into_iter().collect();
        BoolArray {
            bitmap: Bitmap::ones(data.len()),
            data,
        }
    }
}

impl EstimateSize for BoolArray {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size() + self.data.estimated_heap_size()
    }
}

impl Array for BoolArray {
    type Builder = BoolArrayBuilder;
    type OwnedItem = bool;
    type RefItem<'a> = bool;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> bool {
        self.data.is_set_unchecked(idx)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let value = self.data.to_protobuf();
        let null_bitmap = self.null_bitmap().to_protobuf();

        PbArray {
            null_bitmap: Some(null_bitmap),
            values: vec![value],
            array_type: ArrayType::Bool as i32,
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

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        let array_builder = BoolArrayBuilder::new(capacity);
        ArrayBuilderImpl::Bool(array_builder)
    }
}

/// `BoolArrayBuilder` constructs a `BoolArray` from `Option<Bool>`.
#[derive(Debug)]
pub struct BoolArrayBuilder {
    bitmap: BitmapBuilder,
    data: BitmapBuilder,
}

impl ArrayBuilder for BoolArrayBuilder {
    type ArrayType = BoolArray;

    fn with_meta(capacity: usize, _meta: ArrayMeta) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: BitmapBuilder::with_capacity(capacity),
        }
    }

    fn append_n(&mut self, n: usize, value: Option<bool>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data.append_n(n, x);
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data.append_n(n, false);
            }
        }
    }

    fn append_array(&mut self, other: &BoolArray) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }

        for bit in other.data.iter() {
            self.data.append(bit);
        }
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn finish(self) -> BoolArray {
        BoolArray {
            bitmap: self.bitmap.finish(),
            data: self.data.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use itertools::Itertools;

    use super::*;
    use crate::array::{read_bool_array, NULL_VAL_FOR_HASH};
    use crate::util::iter_util::ZipEqFast;

    fn helper_test_builder(data: Vec<Option<bool>>) -> BoolArray {
        let mut builder = BoolArrayBuilder::new(data.len());
        for d in data {
            builder.append(d);
        }
        builder.finish()
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
        assert_eq!(256, array.estimated_heap_size());
        assert_eq!(320, array.estimated_size());
        let res = v.iter().zip_eq_fast(array.iter()).all(|(a, b)| *a == b);
        assert!(res);
    }

    #[test]
    fn test_bool_array_serde() {
        for num_bits in [0..8, 128..136].into_iter().flatten() {
            let v = (0..num_bits)
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

            let encoded = array.to_protobuf();
            let decoded = read_bool_array(&encoded, num_bits).unwrap().into_bool();

            let equal = array
                .iter()
                .zip_eq_fast(decoded.iter())
                .all(|(a, b)| a == b);
            assert!(equal);
        }
    }

    #[test]
    fn test_bool_array_hash() {
        use std::hash::BuildHasher;

        use twox_hash::RandomXxHashBuilder64;

        use super::super::test_util::{hash_finish, test_hash};

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
            v.iter()
                .zip_eq_fast(&mut states)
                .for_each(|(x, state)| match x {
                    Some(inner) => inner.hash(state),
                    None => NULL_VAL_FOR_HASH.hash(state),
                })
        });
        let hashes = hash_finish(&mut states[..]);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 6);

        test_hash(arrs, hashes, hasher_builder);
    }
}
