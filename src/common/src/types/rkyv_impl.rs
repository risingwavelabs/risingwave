use rkyv::primitive::{
    ArchivedF32, ArchivedF64, ArchivedI16, ArchivedI32, ArchivedI64, ArchivedI128,
};
use rkyv::string::ArchivedString;
use rkyv::with::AsString;
use rkyv::{Archive, Portable, Serialize};

fn __test(my: My<'static>) {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&my).unwrap();
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
            ArchivedMy::Int16(i16_le) => todo!(),
            ArchivedMy::Int32(i32_le) => todo!(),
            ArchivedMy::Int64(i64_le) => todo!(),
            ArchivedMy::Int256(_) => todo!(),
            ArchivedMy::Float32(f32_le) => todo!(),
            ArchivedMy::Float64(f64_le) => todo!(),
            ArchivedMy::Utf8(archived_string) => ScalarRefImpl::Utf8(archived_string.as_str()),
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
pub use general::WithDelegate;

use crate::types::ScalarRefImpl;

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

    // impl<T: Archive> rkyv::with::ArchiveWith<OrderedFloat<T>> for With {
    //     type Archived = <T as Archive>::Archived;
    //     type Resolver = <T as Archive>::Resolver;

    //     fn resolve_with(
    //         field: &OrderedFloat<T>,
    //         resolver: Self::Resolver,
    //         out: rkyv::Place<Self::Archived>,
    //     ) {
    //         field.0.resolve(resolver, out)
    //     }
    // }

    // impl<T: Serialize<S>, S: Fallible> rkyv::with::SerializeWith<OrderedFloat<T>, S> for With {
    //     fn serialize_with(
    //         field: &OrderedFloat<T>,
    //         serializer: &mut S,
    //     ) -> Result<Self::Resolver, <S as rkyv::rancor::Fallible>::Error> {
    //         field.0.serialize(serializer)
    //     }
    // }
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

    // pub(super) struct With;

    // impl rkyv::with::ArchiveWith<crate::types::Int256Ref<'_>> for With {
    //     type Archived = [ArchivedI128; 2];
    //     type Resolver = [(); 2];

    //     fn resolve_with(
    //         field: &crate::types::Int256Ref<'_>,
    //         resolver: Self::Resolver,
    //         out: rkyv::Place<Self::Archived>,
    //     ) {
    //         let words = field.0.0;
    //         words.resolve(resolver, out)
    //     }
    // }
}
