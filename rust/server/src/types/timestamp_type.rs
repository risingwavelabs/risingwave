use crate::array::{BoxedArrayBuilder, PrimitiveArrayBuilder};
use crate::error::Result;
use crate::types::DataTypeKind;
use crate::types::{DataType, PrimitiveDataType};
use chrono::FixedOffset;
use risingwave_proto::data::{DataType as DataTypeProto, DataType_TypeName};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct TimestampType {
    nullable: bool,
    // precision is bound to 0 ~ 6 for timestamp
    precision: u32,
    // Only store utc timezone internally. each timestamp is map to a offset of utc.
    timezone: FixedOffset,
}

impl DataType for TimestampType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Boolean
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(self: Arc<Self>, _capacity: usize) -> Result<BoxedArrayBuilder> {
        Ok(Box::new(PrimitiveArrayBuilder::<TimestampType>::new(
            self, _capacity,
        )))
    }

    fn to_protobuf(&self) -> Result<DataTypeProto> {
        let mut proto = DataTypeProto::new();
        proto.set_type_name(DataType_TypeName::TIMESTAMP);
        proto.set_is_nullable(self.nullable);
        proto.set_precision(self.precision);
        Ok(proto)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Default for TimestampType {
    fn default() -> Self {
        Self {
            nullable: false,
            precision: 0,
            timezone: FixedOffset::east(0),
        }
    }
}

impl PrimitiveDataType for TimestampType {
    const DATA_TYPE_KIND: DataTypeKind = DataTypeKind::Timestamp;
    type N = i64;
}
