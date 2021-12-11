use std::any::Any;
use std::convert::TryFrom;
use std::default::Default;
use std::mem::size_of;
use std::sync::Arc;

use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as ProstDataType;

use crate::array::{ArrayBuilder, ArrayBuilderImpl, PrimitiveArrayBuilder};
use crate::error::{Result, RwError};
use crate::types::{DataSize, DataType, DataTypeKind, DataTypeRef};

const LEAP_DAYS: &[i32] = &[0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const NORMAL_DAYS: &[i32] = &[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}
// return the days of the `year-month`
pub fn get_mouth_days(year: i32, month: usize) -> i32 {
    if is_leap_year(year) {
        LEAP_DAYS[month]
    } else {
        NORMAL_DAYS[month]
    }
}

/// Generate macros for Time/Timestamp/Timestamp with Timezone.
/// FIXME: This code is adapted from numeric type. Maybe we should unify them
macro_rules! make_datetime_type {
    ($name:ident, $native_ty:ty, $data_ty:expr, $proto_ty:expr) => {
        #[derive(Debug)]
        pub struct $name {
            nullable: bool,
            precision: u32,
        }

        impl $name {
            pub fn new(nullable: bool, precision: u32) -> Self {
                Self {
                    nullable,
                    precision,
                }
            }

            pub fn create(nullable: bool, precision: u32) -> DataTypeRef {
                Arc::new(Self::new(nullable, precision))
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    nullable: false,
                    precision: 0,
                }
            }
        }

        impl DataType for $name {
            fn data_type_kind(&self) -> DataTypeKind {
                $data_ty
            }

            fn is_nullable(&self) -> bool {
                self.nullable
            }

            fn create_array_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
                Ok(PrimitiveArrayBuilder::<$native_ty>::new(capacity)?.into())
            }

            fn to_protobuf(&self) -> Result<ProstDataType> {
                let proto = ProstDataType {
                    type_name: $proto_ty as i32,
                    precision: self.precision,
                    is_nullable: self.nullable,
                    ..Default::default()
                };
                Ok(proto)
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn data_size(&self) -> DataSize {
                DataSize::Fixed(size_of::<$native_ty>())
            }
        }

        impl<'a> TryFrom<&'a ProstDataType> for $name {
            type Error = RwError;

            fn try_from(proto: &'a ProstDataType) -> Result<Self> {
                ensure!(proto.get_type_name() == $proto_ty);
                Ok(Self {
                    nullable: proto.is_nullable,
                    precision: proto.precision,
                })
            }
        }
    };
}

make_datetime_type!(TimeType, i64, DataTypeKind::Time, TypeName::Time);
make_datetime_type!(
    TimestampType,
    i64,
    DataTypeKind::Timestamp,
    TypeName::Timestamp
);
make_datetime_type!(
    TimestampWithTimeZoneType,
    i64,
    DataTypeKind::Timestampz,
    TypeName::Timestampz
);
