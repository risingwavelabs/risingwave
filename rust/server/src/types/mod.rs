use crate::error::Result;
use risingwave_proto::data::DataType as DataTypeProto;
use std::sync::Arc;

mod numeric;
pub(crate) use numeric::*;
mod primitive;
pub(crate) use primitive::*;
mod native;
use crate::array::BoxedArrayBuilder;
use crate::error::ErrorCode::InternalError;
pub(crate) use native::*;
use risingwave_proto::data::DataType_TypeName::DOUBLE;
use risingwave_proto::data::DataType_TypeName::FLOAT;
use risingwave_proto::data::DataType_TypeName::INT16;
use risingwave_proto::data::DataType_TypeName::INT32;
use risingwave_proto::data::DataType_TypeName::INT64;
use std::convert::TryFrom;

mod bool;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub(crate) enum DataTypeKind {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
}

pub(crate) trait DataType: Sync + Send + 'static {
    fn data_type_kind(&self) -> DataTypeKind;
    fn is_nullable(&self) -> bool;
    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<BoxedArrayBuilder>;
    fn to_protobuf(&self) -> Result<DataTypeProto>;
}

pub(crate) type DataTypeRef = Arc<dyn DataType>;

macro_rules! build_data_type {
  ($proto: expr, $($proto_type_name:path => $data_type:ty),*) => {
    match $proto.get_type_name() {
      $(
        $proto_type_name => {
          <$data_type>::try_from($proto).map(|d| Arc::new(d) as DataTypeRef)
        },
      )*
      _ => Err(InternalError(format!("Unsupported proto type: {:?}", $proto.get_type_name())).into())
    }
  }
}

pub(crate) fn build_from_proto(proto: &DataTypeProto) -> Result<DataTypeRef> {
    build_data_type! {
      proto,
      INT16 => Int16Type,
      INT32 => Int32Type,
      INT64 => Int64Type,
      FLOAT => Float32Type,
      DOUBLE => Float64Type
    }
}
