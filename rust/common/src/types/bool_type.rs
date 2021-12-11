use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as ProstDataType;

use crate::array::{ArrayBuilder, ArrayBuilderImpl, BoolArrayBuilder};
use crate::error::{Result, RwError};
use crate::types::{DataSize, DataType, DataTypeKind, DataTypeRef};

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

    fn create_array_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        BoolArrayBuilder::new(capacity).map(|x| x.into())
    }

    fn to_protobuf(&self) -> Result<ProstDataType> {
        let proto = ProstDataType {
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

impl<'a> TryFrom<&'a ProstDataType> for BoolType {
    type Error = RwError;

    fn try_from(proto: &'a ProstDataType) -> Result<Self> {
        ensure!(proto.get_type_name() == TypeName::Boolean);
        Ok(BoolType::new(proto.get_is_nullable()))
    }
}
