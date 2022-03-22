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
//
use std::hash::{Hash, Hasher};
use std::mem::size_of;

use risingwave_pb::data::buffer::CompressionType;
use risingwave_pb::data::{Array as ProstArray, ArrayType, Buffer};

use crate::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayIterator, ArrayMeta, NULL_VAL_FOR_HASH,
};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;
use crate::for_all_chrono_variants;
use crate::types::{NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper};

macro_rules! get_chrono_array {
    ($( { $variant_name:ty, $chrono:ident, $array:ident, $builder:ident } ),*) => {
        $(
            #[derive(Debug)]
            pub struct $array {
                bitmap: Bitmap,
                data: Vec<$variant_name>
            }

            impl $array {
                pub fn from_slice(data: &[Option<$variant_name>]) -> Result<Self> {
                    let mut builder = <Self as Array>::Builder::new(data.len())?;
                    for i in data {
                        builder.append(*i)?;
                    }
                    builder.finish()
                }
            }

            impl Array for $array {
                type Builder = $builder;
                type RefItem<'a> = $variant_name;
                type OwnedItem = $variant_name;
                type Iter<'a> = ArrayIterator<'a, Self>;

                fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>> {
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

                fn to_protobuf(&self) -> ProstArray {
                    let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<usize>());

                    for v in self.iter() {
                        v.map(|node| node.to_protobuf(&mut output_buffer));
                    }

                    let buffer = Buffer {
                        compression: CompressionType::None as i32,
                        body: output_buffer,
                    };
                    let null_bitmap = self.null_bitmap().to_protobuf();
                    ProstArray {
                        null_bitmap: Some(null_bitmap),
                        values: vec![buffer],
                        array_type: Self::get_array_type() as i32,
                        struct_array_data: None,
                        list_array_data: None,
                    }
                }

                fn null_bitmap(&self) -> &Bitmap {
                    &self.bitmap
                }

                fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
                    if !self.is_null(idx) {
                        self.data[idx].hash(state);
                    } else {
                        NULL_VAL_FOR_HASH.hash(state);
                    }
                }

                fn create_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
                    let array_builder = $builder::new(capacity)?;
                    Ok(ArrayBuilderImpl::$chrono(array_builder))
                }
            }

            #[derive(Debug)]
            pub struct $builder {
                bitmap: BitmapBuilder,
                data: Vec<$variant_name>
            }

            impl ArrayBuilder for $builder {
                type ArrayType = $array;

                fn new_with_meta(capacity: usize, _meta: ArrayMeta) -> Result<Self> {
                    Ok(Self {
                        bitmap: BitmapBuilder::with_capacity(capacity),
                        data: Vec::with_capacity(capacity),
                    })
                }

                fn append(&mut self, value: Option<$variant_name>) -> Result<()> {
                    match value {
                        Some(x) => {
                            self.bitmap.append(true);
                            self.data.push(x);
                        }
                        None => {
                            self.bitmap.append(false);
                            self.data.push(<$variant_name>::default())
                        }
                    }
                    Ok(())
                }

                fn append_array(&mut self, other: &Self::ArrayType) -> Result<()> {
                    for bit in other.bitmap.iter() {
                        self.bitmap.append(bit);
                    }
                    self.data.extend_from_slice(&other.data);
                    Ok(())
                }

                fn finish(mut self) -> Result<Self::ArrayType> {
                    Ok(Self::ArrayType {
                        bitmap: self.bitmap.finish(),
                        data: self.data,
                    })
                }
            }
        )*
    };
}

for_all_chrono_variants! { get_chrono_array }

impl NaiveDateArray {
    fn get_array_type() -> ArrayType {
        ArrayType::Date
    }
}

impl NaiveDateTimeArray {
    fn get_array_type() -> ArrayType {
        ArrayType::Timestamp
    }
}

impl NaiveTimeArray {
    fn get_array_type() -> ArrayType {
        ArrayType::Time
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_naivedate_builder() {
        let v = (0..1000)
            .map(NaiveDateWrapper::new_with_days)
            .map(|x| x.ok())
            .collect_vec();
        let mut builder = NaiveDateArrayBuilder::new(0).unwrap();
        for i in &v {
            builder.append(*i).unwrap();
        }
        let a = builder.finish().unwrap();
        let res = v.iter().zip_eq(a.iter()).all(|(a, b)| *a == b);
        assert!(res)
    }

    #[test]
    fn test_naivedate_array_to_protobuf() {
        let input = vec![
            NaiveDateWrapper::new_with_days(12345).ok(),
            None,
            NaiveDateWrapper::new_with_days(67890).ok(),
        ];

        let array = NaiveDateArray::from_slice(&input).unwrap();
        let buffers = array.to_protobuf().values;

        assert_eq!(buffers.len(), 1);

        let output_buffer = input.iter().fold(Vec::new(), |mut v, d| match d {
            Some(d) => {
                d.to_protobuf(&mut v).unwrap();
                v
            }
            None => v,
        });

        assert_eq!(buffers[0].get_body(), &output_buffer);
    }
}
