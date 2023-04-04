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

use std::io::{Cursor, Read};

use ethnum::{I256, U256};
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::Buffer;
use risingwave_pb::data::PbArray;

use crate::array::{Array, ArrayBuilder, ArrayImpl, ArrayResult};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::types::num256::{Int256, Int256Ref, Uint256, Uint256Ref};
use crate::types::Scalar;

#[derive(Debug)]
pub struct Int256ArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<I256>,
}

#[derive(Debug, Clone)]
pub struct Int256Array {
    bitmap: Bitmap,
    data: Vec<I256>,
}

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

#[rustfmt::skip]
macro_rules! impl_array_for_num256 {
    (
        $array:ty,
        $array_builder:ty,
        $scalar:ident,
        $scalar_ref:ident < $gen:tt > ,
        $variant_name:ident
    ) => {
        impl Array for $array {
            type Builder = $array_builder;
            type OwnedItem = $scalar;
            type RefItem<$gen> = $scalar_ref<$gen>;

            unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
                $scalar_ref(self.data.get_unchecked(idx))
            }

            fn len(&self) -> usize {
                self.data.len()
            }

            fn to_protobuf(&self) -> PbArray {
                let mut output_buffer = Vec::<u8>::with_capacity(self.len() * $scalar::size());

                for v in self.iter() {
                    v.map(|node| node.to_protobuf(&mut output_buffer));
                }

                let buffer = Buffer {
                    compression: CompressionType::None as i32,
                    body: output_buffer,
                };

                PbArray {
                    null_bitmap: Some(self.null_bitmap().to_protobuf()),
                    values: vec![buffer],
                    array_type: $scalar::array_type() as i32,
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

            fn create_builder(&self, capacity: usize) -> super::ArrayBuilderImpl {
                let array_builder = Self::Builder::new(capacity);
                super::ArrayBuilderImpl::$variant_name(array_builder)
            }
        }

        impl ArrayBuilder for $array_builder {
            type ArrayType = $array;

            fn with_meta(capacity: usize, _meta: super::ArrayMeta) -> Self {
                Self {
                    bitmap: BitmapBuilder::with_capacity(capacity),
                    data: Vec::with_capacity(capacity),
                }
            }

            fn append_n(
                &mut self,
                n: usize,
                value: Option<<Self::ArrayType as Array>::RefItem<'_>>,
            ) {
                match value {
                    Some(x) => {
                        self.bitmap.append_n(n, true);
                        self.data
                            .extend(std::iter::repeat(x).take(n).map(|x| x.0.clone()));
                    }
                    None => {
                        self.bitmap.append_n(n, false);
                        self.data
                            .extend(std::iter::repeat($scalar::default().into_inner()).take(n));
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

        impl $array {
            pub fn from_protobuf(array: &PbArray, cardinality:usize) -> ArrayResult<ArrayImpl> {
                ensure!(
                    array.get_values().len() == 1,
                    "Must have only 1 buffer in array"
                );

                let buf = array.get_values()[0].get_body().as_slice();

                let mut builder = <$array_builder>::new(cardinality);
                let bitmap: Bitmap = array.get_null_bitmap()?.into();
                let mut cursor = Cursor::new(buf);
                for not_null in bitmap.iter() {
                    if not_null {
                        let mut buf = [0u8; $scalar::size()];
                        cursor.read_exact(&mut buf)?;
                        let item = <$scalar>::from_be_bytes(buf);
                        builder.append(Some(item.as_scalar_ref()));
                    } else {
                        builder.append(None);
                    }
                }
                let arr = builder.finish();
                ensure_eq!(arr.len(), cardinality);

                Ok(arr.into())
            }
        }

    };
}

impl_array_for_num256!(
    Uint256Array,
    Uint256ArrayBuilder,
    Uint256,
    Uint256Ref<'a>,
    Uint256
);

impl_array_for_num256!(
    Int256Array,
    Int256ArrayBuilder,
    Int256,
    Int256Ref<'a>,
    Int256
);
