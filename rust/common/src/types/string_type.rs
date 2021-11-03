use crate::array::{ArrayBuilder, ArrayBuilderImpl, UTF8ArrayBuilder};
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::types::{DataSize, DataType, DataTypeKind, DataTypeRef};
use risingwave_pb::data::{data_type::TypeName, DataType as DataTypeProto};
use risingwave_pb::ToProto;
use std::any::Any;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq)]
pub struct StringType {
    nullable: bool,
    width: usize,
    kind: DataTypeKind,
}

impl DataType for StringType {
    fn data_type_kind(&self) -> DataTypeKind {
        self.kind
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<ArrayBuilderImpl> {
        Ok(UTF8ArrayBuilder::new(capacity)?.into())
    }

    fn to_protobuf(&self) -> Result<risingwave_proto::data::DataType> {
        self.to_prost()
            .map(|x| x.to_proto::<risingwave_proto::data::DataType>())
    }

    fn to_prost(&self) -> Result<DataTypeProto> {
        let mut proto = DataTypeProto {
            precision: self.width as u32,
            is_nullable: self.nullable,
            ..Default::default()
        };

        match self.kind {
            DataTypeKind::Char => {
                proto.set_type_name(TypeName::Char);
                Ok(proto)
            }
            DataTypeKind::Varchar => {
                proto.set_type_name(TypeName::Varchar);
                Ok(proto)
            }
            _ => Err(InternalError(format!(
                "Incorrect data type kind for string type: {:?}",
                self.kind
            ))
            .into()),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_size(&self) -> DataSize {
        DataSize::Variable
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for StringType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        match proto.get_type_name() {
            TypeName::Char => Ok(Self {
                nullable: proto.is_nullable,
                width: proto.precision as usize,
                kind: DataTypeKind::Char,
            }),
            TypeName::Varchar => Ok(Self {
                nullable: proto.is_nullable,
                width: proto.precision as usize,
                kind: DataTypeKind::Varchar,
            }),
            _ => Err(InternalError(format!(
                "Incorrect data type kind for string type: {:?}",
                proto.get_type_name()
            ))
            .into()),
        }
    }
}

impl StringType {
    pub fn create(nullable: bool, width: usize, kind: DataTypeKind) -> DataTypeRef {
        Arc::new(Self {
            nullable,
            width,
            kind,
        }) as DataTypeRef
    }
}
