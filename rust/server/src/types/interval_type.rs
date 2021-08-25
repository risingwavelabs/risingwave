use crate::array::{BoxedArrayBuilder, PrimitiveArrayBuilder};
use crate::error::{Result, RwError};
use crate::types::numeric_type::*;
use crate::types::{DataType, DataTypeKind};
use risingwave_proto::data::{DataType as DataTypeProto, DataType_IntervalType, DataType_TypeName};
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;
#[derive(Debug)]
struct IntervalType {
    nullable: bool,
    inner_type: IntervalUnit,
    // Used for precision of seconds.
    precision: u32,
}

#[derive(Copy, Clone, Debug)]
enum IntervalUnit {
    Year,
    Day,
    // TODO: other fileds in https://www.postgresql.org/docs/current/datatype-datetime.html
}

impl DataType for IntervalType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Interval
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(
        self: Arc<Self>,
        capacity: usize,
    ) -> crate::error::Result<BoxedArrayBuilder> {
        Ok(Box::new(match self.inner_type {
            // FIXME: change signed integer to unsigned.
            IntervalUnit::Year => PrimitiveArrayBuilder::<Int32Type>::new(
                Arc::new(Int32Type::new(self.nullable)),
                capacity,
            ),
            IntervalUnit::Day => PrimitiveArrayBuilder::<Int32Type>::new(
                Arc::new(Int32Type::new(self.nullable)),
                capacity,
            ),
        }))
    }

    fn to_protobuf(&self) -> crate::error::Result<DataTypeProto> {
        let mut proto = DataTypeProto::new();
        proto.set_type_name(DataType_TypeName::INTERVAL);
        proto.set_interval_type(IntervalType::as_proto_interval_type(self.inner_type));
        proto.set_is_nullable(self.nullable);
        proto.set_precision(self.precision);
        Ok(proto)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl IntervalType {
    fn as_proto_interval_type(interval_type: IntervalUnit) -> DataType_IntervalType {
        match interval_type {
            IntervalUnit::Day => DataType_IntervalType::DAY,
            IntervalUnit::Year => DataType_IntervalType::YEAR,
        }
    }

    fn from_proto_interval_type(proto_interval: DataType_IntervalType) -> IntervalUnit {
        match proto_interval {
            DataType_IntervalType::YEAR => IntervalUnit::Year,
            DataType_IntervalType::DAY => IntervalUnit::Day,
            _ => unimplemented!(),
        }
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for IntervalType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        ensure!(proto.get_type_name() == DataType_TypeName::INTERVAL);
        Ok(Self {
            nullable: proto.get_is_nullable(),
            precision: proto.get_precision(),
            inner_type: IntervalType::from_proto_interval_type(proto.get_interval_type()),
        })
    }
}
