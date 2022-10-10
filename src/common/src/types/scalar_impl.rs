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

use super::*;
use crate::array::list_array::{ListRef, ListValue};
use crate::array::struct_array::{StructRef, StructValue};
use crate::{for_all_native_types, for_all_scalar_variants};

/// `ScalarPartialOrd` allows comparison between `Scalar` and `ScalarRef`.
///
/// TODO: see if it is possible to implement this trait directly on `ScalarRef`.
pub trait ScalarPartialOrd: Scalar {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering>;
}

/// Implement `Scalar` and `ScalarRef` for native type.
/// For `PrimitiveArrayItemType`, clone is trivial, so `T` is both `Scalar` and `ScalarRef`.
macro_rules! impl_all_native_scalar {
    ($({ $scalar_type:ty, $variant_name:ident } ),*) => {
        $(
            impl Scalar for $scalar_type {
                type ScalarRefType<'a> = Self;

                fn as_scalar_ref(&self) -> Self {
                    *self
                }

                fn to_scalar_value(self) -> ScalarImpl {
                    ScalarImpl::$variant_name(self)
                }
            }

            impl<'scalar> ScalarRef<'scalar> for $scalar_type {
                type ScalarType = Self;

                fn to_owned_scalar(&self) -> Self {
                    *self
                }
            }
        )*
    };
}

for_all_native_types! { impl_all_native_scalar }

/// Implement `Scalar` for `String`.
/// `String` could be converted to `&str`.
impl Scalar for String {
    type ScalarRefType<'a> = &'a str;

    fn as_scalar_ref(&self) -> &str {
        self.as_str()
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Utf8(self)
    }
}

/// Implement `Scalar` for `StructValue`.
impl Scalar for StructValue {
    type ScalarRefType<'a> = StructRef<'a>;

    fn as_scalar_ref(&self) -> StructRef<'_> {
        StructRef::ValueRef { val: self }
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Struct(self)
    }
}

/// Implement `Scalar` for `ListValue`.
impl Scalar for ListValue {
    type ScalarRefType<'a> = ListRef<'a>;

    fn as_scalar_ref(&self) -> ListRef<'_> {
        ListRef::ValueRef { val: self }
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::List(self)
    }
}

/// Implement `ScalarRef` for `String`.
/// `String` could be converted to `&str`.
impl<'a> ScalarRef<'a> for &'a str {
    type ScalarType = String;

    fn to_owned_scalar(&self) -> String {
        self.to_string()
    }
}

impl ScalarPartialOrd for Decimal {
    fn scalar_cmp(&self, other: Self) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

impl ScalarPartialOrd for String {
    fn scalar_cmp(&self, other: &str) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(other)
    }
}

impl<T: PrimitiveArrayItemType + Scalar> ScalarPartialOrd for T {
    fn scalar_cmp(&self, other: Self) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

impl ScalarPartialOrd for bool {
    fn scalar_cmp(&self, other: Self) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

/// Implement `Scalar` for `bool`.
impl Scalar for bool {
    type ScalarRefType<'a> = bool;

    fn as_scalar_ref(&self) -> bool {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Bool(self)
    }
}

/// Implement `Scalar` and `ScalarRef` for `String`.
/// `String` could be converted to `&str`.
impl<'a> ScalarRef<'a> for bool {
    type ScalarType = bool;

    fn to_owned_scalar(&self) -> bool {
        *self
    }
}

/// Implement `Scalar` for `Decimal`.
impl Scalar for Decimal {
    type ScalarRefType<'a> = Decimal;

    fn as_scalar_ref(&self) -> Decimal {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Decimal(self)
    }
}

/// Implement `ScalarRef` for `Decimal`.
impl<'a> ScalarRef<'a> for Decimal {
    type ScalarType = Decimal;

    fn to_owned_scalar(&self) -> Decimal {
        *self
    }
}

/// Implement `Scalar` for `IntervalUnit`.
impl Scalar for IntervalUnit {
    type ScalarRefType<'a> = IntervalUnit;

    fn as_scalar_ref(&self) -> IntervalUnit {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Interval(self)
    }
}

/// Implement `ScalarRef` for `IntervalUnit`.
impl<'a> ScalarRef<'a> for IntervalUnit {
    type ScalarType = IntervalUnit;

    fn to_owned_scalar(&self) -> IntervalUnit {
        *self
    }
}

/// Implement `Scalar` for `NaiveDateWrapper`.
impl Scalar for NaiveDateWrapper {
    type ScalarRefType<'a> = NaiveDateWrapper;

    fn as_scalar_ref(&self) -> NaiveDateWrapper {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::NaiveDate(self)
    }
}

/// Implement `ScalarRef` for `NaiveDateWrapper`.
impl<'a> ScalarRef<'a> for NaiveDateWrapper {
    type ScalarType = NaiveDateWrapper;

    fn to_owned_scalar(&self) -> NaiveDateWrapper {
        *self
    }
}

/// Implement `Scalar` for `NaiveDateTimeWrapper`.
impl Scalar for NaiveDateTimeWrapper {
    type ScalarRefType<'a> = NaiveDateTimeWrapper;

    fn as_scalar_ref(&self) -> NaiveDateTimeWrapper {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::NaiveDateTime(self)
    }
}

/// Implement `ScalarRef` for `NaiveDateTimeWrapper`.
impl<'a> ScalarRef<'a> for NaiveDateTimeWrapper {
    type ScalarType = NaiveDateTimeWrapper;

    fn to_owned_scalar(&self) -> NaiveDateTimeWrapper {
        *self
    }
}

/// Implement `Scalar` for `NaiveTimeWrapper`.
impl Scalar for NaiveTimeWrapper {
    type ScalarRefType<'a> = NaiveTimeWrapper;

    fn as_scalar_ref(&self) -> NaiveTimeWrapper {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::NaiveTime(self)
    }
}

/// Implement `ScalarRef` for `NaiveTimeWrapper`.
impl<'a> ScalarRef<'a> for NaiveTimeWrapper {
    type ScalarType = NaiveTimeWrapper;

    fn to_owned_scalar(&self) -> NaiveTimeWrapper {
        *self
    }
}

/// Implement `Scalar` for `StructValue`.
impl<'a> ScalarRef<'a> for StructRef<'a> {
    type ScalarType = StructValue;

    fn to_owned_scalar(&self) -> StructValue {
        let fields = self
            .fields_ref()
            .iter()
            .map(|f| f.map(|s| s.into_scalar_impl()))
            .collect();
        StructValue::new(fields)
    }
}

/// Implement `Scalar` for `ListValue`.
impl<'a> ScalarRef<'a> for ListRef<'a> {
    type ScalarType = ListValue;

    fn to_owned_scalar(&self) -> ListValue {
        let fields = self
            .values_ref()
            .iter()
            .map(|f| f.map(|s| s.into_scalar_impl()))
            .collect();
        ListValue::new(fields)
    }
}

impl ScalarImpl {
    pub fn get_ident(&self) -> &'static str {
        macro_rules! impl_all_get_ident {
            ($({ $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match self {
                    $( Self::$variant_name(_) => stringify!($variant_name), )*
                }
            };
        }
        for_all_scalar_variants! { impl_all_get_ident }
    }
}

impl<'scalar> ScalarRefImpl<'scalar> {
    pub fn get_ident(&self) -> &'static str {
        macro_rules! impl_all_get_ident {
            ($({ $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match self {
                    $( Self::$variant_name(_) => stringify!($variant_name), )*
                }
            };
        }
        for_all_scalar_variants! { impl_all_get_ident }
    }
}
