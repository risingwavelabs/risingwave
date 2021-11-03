use crate::array::{ArrayBuilder, ArrayBuilderImpl, BoolArrayBuilder};
use crate::error::{Result, RwError};
use crate::types::{DataSize, DataType, DataTypeKind, DataTypeRef};
use risingwave_pb::data::{data_type::TypeName, DataType as DataTypeProto};
use risingwave_pb::ToProto;
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

    fn to_protobuf(&self) -> Result<risingwave_proto::data::DataType> {
        self.to_prost()
            .map(|x| x.to_proto::<risingwave_proto::data::DataType>())
    }

    fn to_prost(&self) -> Result<DataTypeProto> {
        let proto = DataTypeProto {
            type_name: TypeName::Boolean as i32,
            is_nullable: self.nullable,
            ..Default::default()
        };
        Ok(proto)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_size(&self) -> DataSize {
        DataSize::Variable
    }
}

impl BoolType {
    pub fn new(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn create(nullable: bool) -> DataTypeRef {
        Arc::new(Self::new(nullable))
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for BoolType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        ensure!(proto.get_type_name() == TypeName::Boolean);
        Ok(BoolType::new(proto.get_is_nullable()))
    }
}
