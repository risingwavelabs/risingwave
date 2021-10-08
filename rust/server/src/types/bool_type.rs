use crate::array2::{ArrayBuilder, ArrayBuilderImpl, BoolArrayBuilder};
use crate::error::{Result, RwError};
use crate::types::{DataType, DataTypeKind, DataTypeRef};
use risingwave_proto::data::{DataType as DataTypeProto, DataType_TypeName};
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

/// `BoolType` is not a primitive type because we use a bit for each bool value, not a [`bool`].
#[derive(Debug, Eq, PartialEq)]
pub struct BoolType {
    nullable: bool,
}

impl DataType for BoolType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Boolean
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<ArrayBuilderImpl> {
        BoolArrayBuilder::new(capacity).map(|x| x.into())
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
    pub fn new(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn create(nullable: bool) -> DataTypeRef {
        Arc::new(BoolType::new(nullable)) as DataTypeRef
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for BoolType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        ensure!(proto.get_type_name() == DataType_TypeName::BOOLEAN);
        Ok(BoolType::new(proto.get_is_nullable()))
    }
}
