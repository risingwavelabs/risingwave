// Copyright 2022 RisingWave Labs
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

use std::fmt::Debug;

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::common::Buffer;
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::{Array, ArrayBuilder};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, Decimal};

/// `DecimalArray` is a collection of `Decimal`.
#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct DecimalArray {
    bitmap: Bitmap,
    data: Box<[Decimal]>,
}

impl FromIterator<Option<Decimal>> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = Option<Decimal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl<'a> FromIterator<&'a Option<Decimal>> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = &'a Option<Decimal>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl FromIterator<Decimal> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = Decimal>>(iter: I) -> Self {
        let data: Box<[Decimal]> = iter.into_iter().collect();
        DecimalArray {
            bitmap: Bitmap::ones(data.len()),
            data,
        }
    }
}

impl Array for DecimalArray {
    type Builder = DecimalArrayBuilder;
    type OwnedItem = Decimal;
    type RefItem<'a> = Decimal;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        unsafe { *self.data.get_unchecked(idx) }
    }

    fn raw_iter(&self) -> impl ExactSizeIterator<Item = Self::RefItem<'_>> {
        self.data.iter().cloned()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<Decimal>());

        for v in self.iter() {
            v.map(|node| node.to_protobuf(&mut output_buffer));
        }

        let buffer = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };
        let null_bitmap = self.null_bitmap().to_protobuf();
        PbArray {
            null_bitmap: Some(null_bitmap),
            values: vec![buffer],
            array_type: PbArrayType::Decimal as i32,
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

    fn data_type(&self) -> DataType {
        DataType::Decimal
    }
}

/// `DecimalArrayBuilder` constructs a `DecimalArray` from `Option<Decimal>`.
#[derive(Debug, Clone, EstimateSize)]
pub struct DecimalArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<Decimal>,
}

impl ArrayBuilder for DecimalArrayBuilder {
    type ArrayType = DecimalArray;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Decimal);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<Decimal>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data.extend(std::iter::repeat_n(x, n));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data.extend(std::iter::repeat_n(Decimal::default(), n));
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

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> DecimalArray {
        DecimalArray {
            bitmap: self.bitmap.finish(),
            data: self.data.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;
    use std::str::FromStr;

    use itertools::Itertools;

    use super::*;
    use crate::array::{Array, ArrayBuilder, ArrayImpl, NULL_VAL_FOR_HASH};
    use crate::util::iter_util::ZipEqFast;

    #[test]
    fn test_decimal_builder() {
        let v = (0..1000).map(Decimal::from).collect_vec();
        let mut builder = DecimalArrayBuilder::new(0);
        for i in &v {
            builder.append(Some(*i));
        }
        let a = builder.finish();
        let res = v.iter().zip_eq_fast(a.iter()).all(|(a, b)| Some(*a) == b);
        assert!(res);
    }

    #[test]
    fn test_decimal_array_to_protobuf() {
        let input = vec![
            Some(Decimal::from_str("1.01").unwrap()),
            Some(Decimal::from_str("2.02").unwrap()),
            None,
            Some(Decimal::from_str("4.04").unwrap()),
            None,
            Some(Decimal::NegativeInf),
            Some(Decimal::PositiveInf),
            Some(Decimal::NaN),
        ];

        let array = DecimalArray::from_iter(&input);
        let prost_array = array.to_protobuf();

        assert_eq!(prost_array.values.len(), 1);

        let decoded_array = ArrayImpl::from_protobuf(&prost_array, 8)
            .unwrap()
            .into_decimal();

        assert!(array.iter().eq(decoded_array.iter()));
    }

    #[test]
    fn test_decimal_array_hash() {
        use std::hash::BuildHasher;

        use super::super::test_util::{hash_finish, test_hash};

        const ARR_NUM: usize = 3;
        const ARR_LEN: usize = 270;
        let vecs: [Vec<Option<Decimal>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Some(Decimal::from(0)),
                    1 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Some(Decimal::from(0)),
                    #[expect(clippy::approx_constant)]
                    1 => Decimal::try_from(3.14).ok(),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 5 {
                    0 => Some(Decimal::from(0)),
                    1 => Some(Decimal::from(123)),
                    #[expect(clippy::approx_constant)]
                    2 => Decimal::try_from(3.1415926).ok(),
                    #[expect(clippy::approx_constant)]
                    3 => Decimal::try_from(3.14).ok(),
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

        let hasher_builder = twox_hash::xxhash64::RandomState::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter()
                .zip_eq_fast(&mut states)
                .for_each(|(x, state)| match x {
                    Some(inner) => inner.hash(state),
                    None => NULL_VAL_FOR_HASH.hash(state),
                })
        });
        let hashes = hash_finish(&states[..]);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 30);

        test_hash(arrs, hashes, hasher_builder);
    }
}
