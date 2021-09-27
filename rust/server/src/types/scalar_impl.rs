use super::*;

/// `ScalarPartialOrd` allows comparison between `Scalar` and `ScalarRef`.
///
/// TODO: see if it is possible to implement this trait directly on `ScalarRef`.
pub trait ScalarPartialOrd: Scalar {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering>;
}

/// Implement `Scalar` for `PrimitiveArrayItemType`.
/// For PrimitiveArrayItemType, clone is trivial, so `T` is both `Scalar` and `ScalarRef`.
impl<T: PrimitiveArrayItemType> Scalar for T {
    type ScalarRefType<'a> = T;

    fn as_scalar_ref(&self) -> T {
        *self
    }
}

/// Implement `ScalarRef` for `PrimitiveArrayItemType`.
/// For PrimitiveArrayItemType, clone is trivial, so `T` is both `Scalar` and `ScalarRef`.
impl<'a, T: PrimitiveArrayItemType> ScalarRef<'a> for T {
    type ScalarType = T;

    fn to_owned_scalar(&self) -> T {
        *self
    }
}

/// Implement `Scalar` for `String`.
/// `String` could be converted to `&str`.
impl Scalar for String {
    type ScalarRefType<'a> = &'a str;

    fn as_scalar_ref(&self) -> &str {
        self.as_str()
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

impl ScalarPartialOrd for String {
    fn scalar_cmp(&self, other: &str) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(other)
    }
}

impl<T: PrimitiveArrayItemType> ScalarPartialOrd for T {
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
}

/// Implement `Scalar` for `Decimal`.
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
}

/// Implement `Scalar` for `IntervalUnit`.
impl<'a> ScalarRef<'a> for IntervalUnit {
    type ScalarType = IntervalUnit;

    fn to_owned_scalar(&self) -> IntervalUnit {
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
        for_all_variants! { impl_all_get_ident, self }
    }
}
