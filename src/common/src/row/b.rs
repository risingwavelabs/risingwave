use bytes::Buf;

use super::Row;
use crate::array::StructRef;
use crate::for_all_variants;
use crate::types::{Date, DatumRef, ScalarRefImpl};

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! datum_discriminant {
    ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        #[repr(u32)]
        pub enum DatumDiscriminant {
            Null = 0,
            $( $variant_name ),*
        }
    };
}

for_all_variants! { datum_discriminant }

pub struct BRow {
    data: Box<[u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BRowRef<'a> {
    data: &'a [u8],
}

impl<'a> BRowRef<'a> {
    pub fn read_datum(self, index: usize) -> DatumRef<'a> {
        let len = self.len();

        let mut data = self.data;
        data.advance(4 + 8 * index);

        let dis = data.get_u32_ne();
        let dis: DatumDiscriminant = unsafe { std::mem::transmute(dis) };

        // inline
        match dis {
            DatumDiscriminant::Null => {
                return None;
            }
            DatumDiscriminant::Int16 => {
                let scalar = data.get_i32_ne() as i16;
                return Some(ScalarRefImpl::Int16(scalar));
            }
            DatumDiscriminant::Int32 => {
                let scalar = data.get_i32_ne();
                return Some(ScalarRefImpl::Int32(scalar));
            }
            DatumDiscriminant::Float32 => {
                let scalar = data.get_f32_ne().into();
                return Some(ScalarRefImpl::Float32(scalar));
            }
            DatumDiscriminant::Bool => {
                let scalar = data.get_i32_ne() != 0;
                return Some(ScalarRefImpl::Bool(scalar));
            }
            DatumDiscriminant::Date => {
                let days = data.get_i32_ne();
                let date = Date::with_days_since_ce(days).unwrap();
                return Some(ScalarRefImpl::Date(date));
            }
            _ => {}
        };

        let offset = data.get_u32_ne() as usize;

        // skip remaining entries
        data.advance(8 * (len - index - 1));

        // skip to payload
        data.advance(offset);

        let scalar = match dis {
            DatumDiscriminant::Int64 => todo!(),
            DatumDiscriminant::Int256 => todo!(),
            DatumDiscriminant::Float64 => todo!(),
            DatumDiscriminant::Utf8 => {
                let len = data.get_u32_ne() as usize;
                let bytes = &data[..len];
                let str = std::str::from_utf8(bytes).unwrap();
                return Some(ScalarRefImpl::Utf8(str));
            }
            DatumDiscriminant::Decimal => todo!(),
            DatumDiscriminant::Interval => todo!(),
            DatumDiscriminant::Time => todo!(),
            DatumDiscriminant::Timestamp => todo!(),
            DatumDiscriminant::Timestamptz => todo!(),
            DatumDiscriminant::Jsonb => todo!(),
            DatumDiscriminant::Serial => todo!(),
            DatumDiscriminant::Struct => {
                let len = data.get_u32_ne() as usize;
                let bytes = &data[..len];
                let row = BRowRef { data: bytes };
                return Some(ScalarRefImpl::Struct(StructRef::BRow { row }));
            }
            DatumDiscriminant::List => todo!(),
            DatumDiscriminant::Map => todo!(),
            DatumDiscriminant::Bytea => todo!(),
            _ => unreachable!(),
        };

        Some(scalar)
    }

    pub fn iter_datums(self) -> impl ExactSizeIterator<Item = DatumRef<'a>> {
        (0..self.len()).map(move |i| self.read_datum(i))
    }
}

impl<'a> Row for BRowRef<'a> {
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self.read_datum(index)
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.datum_at(index)
    }

    fn len(&self) -> usize {
        (&*self.data).get_u32_ne() as usize
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = DatumRef<'_>> {
        self.iter_datums()
    }
}
