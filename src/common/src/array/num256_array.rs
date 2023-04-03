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

use ethnum::U256;
use risingwave_pb::data::PbArray;

use crate::array::{Array, ArrayBuilder, ArrayImpl, PrimitiveArray, PrimitiveArrayBuilder};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::types::num256::{Int256, Uint256};

// pub type Int256Array = PrimitiveArray<Box<Int256>>;
// pub type Int256ArrayBuilder = PrimitiveArrayBuilder<Box<Int256>>;
//
// pub type Uint256Array = PrimitiveArray<Uint256>;
// pub type Uint256ArrayBuilder = PrimitiveArrayBuilder<Uint256>;
//

#[derive(Debug)]
pub struct Uint256ArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<U256>,
}

#[derive(Debug, Clone)]
pub struct Uint256Array {
    bitmap: Bitmap,
    data: Vec<U256>,
}

#[derive(Debug)]
pub struct Int256ArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<Int256>,
}

#[derive(Debug, Clone)]
pub struct Int256Array {
    bitmap: Bitmap,
    data: Vec<Int256>,
}

impl Array for Int256Array {
    type Builder = Int256ArrayBuilder;
    type OwnedItem = Box<Int256>;
    type RefItem<'a> = &'a Int256;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        self.data.get_unchecked(idx)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> PbArray {
        todo!()
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

    fn create_builder(&self, capacity: usize) -> super::ArrayBuilderImpl {
        let array_builder = Self::Builder::new(capacity);
        super::ArrayBuilderImpl::Int256(array_builder)
    }
}

impl ArrayBuilder for Int256ArrayBuilder {
    type ArrayType = Int256Array;

    fn with_meta(capacity: usize, _meta: super::ArrayMeta) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data
                    .extend(std::iter::repeat(x).take(n).map(|x| x.clone()));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data
                    .extend(std::iter::repeat(Int256::default()).take(n));
            }
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            data: self.data,
        }
    }
}
