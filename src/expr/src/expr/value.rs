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

use either::Either;
use risingwave_common::array::*;
use risingwave_common::for_all_variants;
use risingwave_common::types::{Datum, Scalar};

/// The type-erased return value of an expression.
///
/// It can be either an array, or a scalar if all values in the array are the same.
#[derive(Debug, Clone)]
pub enum ValueImpl {
    Array(ArrayRef),
    Scalar { value: Datum, capacity: usize },
}

/// The generic reference type of [`ValueImpl`]. Used as the arguments of expressions.
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a, A: Array> {
    Array(&'a A),
    Scalar {
        value: Option<<A as Array>::RefItem<'a>>,
        capacity: usize,
    },
}

impl<'a, A: Array> ValueRef<'a, A> {
    /// Iterates over all scalars in this value.
    pub fn iter(self) -> impl Iterator<Item = Option<A::RefItem<'a>>> + 'a {
        match self {
            Self::Array(array) => Either::Left(array.iter()),
            Self::Scalar { value, capacity } => {
                Either::Right(std::iter::repeat(value).take(capacity))
            }
        }
    }
}

macro_rules! impl_convert {
    ($( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        $(
            paste::paste! {
                /// Converts a type-erased value to a reference of a specific array type.
                impl<'a> From<&'a ValueImpl> for ValueRef<'a, $array> {
                    fn from(value: &'a ValueImpl) -> Self {
                        match value {
                            ValueImpl::Array(array) => {
                                let array = array.[<as_ $suffix_name>]();
                                ValueRef::Array(array)
                            },
                            ValueImpl::Scalar { value, capacity } => {
                                let value = value.as_ref().map(|v| v.[<as_ $suffix_name>]().as_scalar_ref());
                                ValueRef::Scalar { value, capacity: *capacity }
                            },
                        }
                    }
                }
            }
        )*
    };
}

for_all_variants! { impl_convert }
