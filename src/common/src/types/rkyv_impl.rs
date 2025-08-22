use general::WithDelegate;
use rkyv::with::AsString;
use rkyv::{Archive, Serialize};

use crate::types::ScalarRefImpl;

fn __test(my: My<'static>) {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&my).unwrap();
    let _scalar = __view(&bytes);
}

fn __view<'a>(data: &'a [u8]) -> ScalarRefImpl<'a> {
    let archived = unsafe { rkyv::access_unchecked::<ArchivedMy<'static>>(data) };
    ScalarRefImpl::from(archived)
}

/// `ScalarRefImpl` embeds all possible scalar references in the evaluation
/// framework.
///
/// Note: `ScalarRefImpl` doesn't contain all information of its `DataType`,
/// so sometimes they need to be used together.
/// e.g., for `Struct`, we don't have the field names in the value.
///
/// See `for_all_variants` for the definition.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Archive, Serialize)]
// #[rkyv(as = ArchivedMy)]
pub enum My<'scalar> {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int256(#[rkyv(with = WithDelegate)] crate::types::Int256Ref<'scalar>),
    Float32(#[rkyv(with = WithDelegate)] crate::types::F32),
    Float64(#[rkyv(with = WithDelegate)] crate::types::F64),
    Utf8(#[rkyv(with = AsString)] &'scalar str),
    // Bool(bool),
    Decimal(#[rkyv(with = WithDelegate)] crate::types::Decimal),
    // Interval(crate::types::Interval),
    // Date(crate::types::Date),
    // Time(crate::types::Time),
    // Timestamp(crate::types::Timestamp),
    // Timestamptz(crate::types::Timestamptz),
    // Jsonb(crate::types::JsonbRef<'scalar>),
    // Serial(crate::types::Serial),
    // Struct(crate::types::StructRef<'scalar>),
    // List(crate::types::ListRef<'scalar>),
    // Map(crate::types::MapRef<'scalar>),
    // Vector(crate::types::VectorRef<'scalar>),
    // Bytea(&'scalar [u8]),
}

// #[derive(Portable)]
// #[repr(u8)]
// pub enum ArchivedMy {
//     Int16(ArchivedI16),
//     Int32(ArchivedI32),
//     Int64(ArchivedI64),
//     Int256([ArchivedI128; 2]),
//     Float32(ArchivedF32),
//     Float64(ArchivedF64),
//     Utf8(ArchivedString),
// }

impl<'a> From<&'a ArchivedMy<'static>> for ScalarRefImpl<'a> {
    fn from(value: &'a ArchivedMy<'static>) -> Self {
        match value {
            ArchivedMy::Int16(i16_le) => Self::Int16(i16_le.to_native()),
            ArchivedMy::Int32(i32_le) => Self::Int32(i32_le.to_native()),
            ArchivedMy::Int64(i64_le) => Self::Int64(i64_le.to_native()),
            ArchivedMy::Int256(words_le) => Self::Int256(
                // UNSAFETY: only works in little endian!
                unsafe { std::mem::transmute(words_le) },
            ),
            ArchivedMy::Float32(f32_le) => Self::Float32(f32_le.to_native().into()),
            ArchivedMy::Float64(f64_le) => Self::Float64(f64_le.to_native().into()),
            ArchivedMy::Utf8(archived_string) => Self::Utf8(archived_string.as_str()),
            ArchivedMy::Decimal(archived_decimal) => Self::Decimal(archived_decimal.into()),
        }
    }
}

mod general {
    use rkyv::rancor::Fallible;
    use rkyv::with::{ArchiveWith, SerializeWith};
    use rkyv::{Archive, Serialize};

    pub trait RkyvDelegate {
        type Target;

        fn delegate(&self) -> Self::Target;
    }

    pub struct WithDelegate;

    impl<T: RkyvDelegate<Target: Archive>> ArchiveWith<T> for WithDelegate {
        type Archived = <T::Target as Archive>::Archived;
        type Resolver = <T::Target as Archive>::Resolver;

        fn resolve_with(field: &T, resolver: Self::Resolver, out: rkyv::Place<Self::Archived>) {
            field.delegate().resolve(resolver, out)
        }
    }

    impl<T: RkyvDelegate<Target: Serialize<S>>, S: Fallible> SerializeWith<T, S> for WithDelegate {
        fn serialize_with(field: &T, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
            field.delegate().serialize(serializer)
        }
    }
}

mod ordered_float {
    use crate::types::ordered_float::OrderedFloat;
    use crate::types::rkyv_impl::general::RkyvDelegate;

    impl RkyvDelegate for OrderedFloat<f32> {
        type Target = f32;

        fn delegate(&self) -> Self::Target {
            self.0
        }
    }
    impl RkyvDelegate for OrderedFloat<f64> {
        type Target = f64;

        fn delegate(&self) -> Self::Target {
            self.0
        }
    }
}

mod int256 {
    use crate::types::Int256Ref;
    use crate::types::rkyv_impl::general::RkyvDelegate;

    impl RkyvDelegate for Int256Ref<'_> {
        type Target = [i128; 2];

        fn delegate(&self) -> Self::Target {
            self.0.0
        }
    }
}

mod decimal {
    use rkyv::{Archive, Serialize};

    use crate::types::Decimal;
    use crate::types::rkyv_impl::general::RkyvDelegate;

    #[derive(Archive, Serialize)]
    pub enum ADecimal {
        Normalized([u8; 16]),
        NaN,
        PositiveInf,
        NegativeInf,
    }

    impl RkyvDelegate for Decimal {
        type Target = ADecimal;

        fn delegate(&self) -> Self::Target {
            match self {
                Decimal::Normalized(d) => ADecimal::Normalized(d.serialize()),
                Decimal::NaN => ADecimal::NaN,
                Decimal::PositiveInf => ADecimal::PositiveInf,
                Decimal::NegativeInf => ADecimal::NegativeInf,
            }
        }
    }

    impl From<&ArchivedADecimal> for Decimal {
        fn from(value: &ArchivedADecimal) -> Self {
            match value {
                ArchivedADecimal::Normalized(bytes) => {
                    Self::Normalized(rust_decimal::Decimal::deserialize(*bytes))
                }
                ArchivedADecimal::NaN => Self::NaN,
                ArchivedADecimal::PositiveInf => Self::PositiveInf,
                ArchivedADecimal::NegativeInf => Self::NegativeInf,
            }
        }
    }
}
