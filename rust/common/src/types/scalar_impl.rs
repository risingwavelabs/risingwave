use super::*;
use crate::array::struct_array::StructValue;
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
  ([], $({ $scalar_type:ty, $variant_name:ident } ),*) => {
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
    type ScalarRefType<'a> = StructValue;

    fn as_scalar_ref(&self) -> StructValue {
        *self
    }

    fn to_scalar_value(self) -> ScalarImpl {
        ScalarImpl::Struct(self)
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

impl ScalarPartialOrd for IntervalUnit {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

impl ScalarPartialOrd for NaiveDateWrapper {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

impl ScalarPartialOrd for NaiveTimeWrapper {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

impl ScalarPartialOrd for NaiveDateTimeWrapper {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
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
impl<'a> ScalarRef<'a> for StructValue {
    type ScalarType = StructValue;

    fn to_owned_scalar(&self) -> StructValue {
        *self
    }
}

impl ScalarImpl {
    pub fn get_ident(&self) -> &'static str {
        macro_rules! impl_all_get_ident {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        match $self {
          $( Self::$variant_name(_) => stringify!($variant_name), )*
        }
      };
    }
        for_all_scalar_variants! { impl_all_get_ident, self }
    }
}

impl<'scalar> ScalarRefImpl<'scalar> {
    pub fn get_ident(&self) -> &'static str {
        macro_rules! impl_all_get_ident {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        match $self {
          $( Self::$variant_name(_) => stringify!($variant_name), )*
        }
      };
    }
        for_all_scalar_variants! { impl_all_get_ident, self }
    }
}
