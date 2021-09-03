use crate::array::{BoolArrayBuilder, BoxedArrayBuilder};
use crate::error::Result;
use crate::types::{DataType, DataTypeKind, DataTypeRef};
use risingwave_proto::data::{DataType as DataTypeProto, DataType_TypeName};
use std::any::Any;
use std::sync::Arc;

/// [BoolType] is not a primitive type because we use a bit for each bool value, not a [bool].
#[derive(Debug)]
pub(crate) struct BoolType {
    nullable: bool,
}

impl DataType for BoolType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Boolean
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(self: Arc<Self>, _capacity: usize) -> Result<BoxedArrayBuilder> {
        BoolArrayBuilder::new(self, _capacity).map(|builder| Box::new(builder) as BoxedArrayBuilder)
    }

    fn to_protobuf(&self) -> Result<DataTypeProto> {
        let mut proto = DataTypeProto::new();
        proto.set_type_name(DataType_TypeName::BOOLEAN);
        proto.set_is_nullable(self.nullable);

        Ok(proto)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl BoolType {
    pub(crate) fn new(nullable: bool) -> Self {
        Self { nullable }
    }

    pub(crate) fn create(nullable: bool) -> DataTypeRef {
        Arc::new(BoolType::new(nullable)) as DataTypeRef
    }
}
