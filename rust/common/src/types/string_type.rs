use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as ProstDataType;

use crate::array::{ArrayBuilder, ArrayBuilderImpl, Utf8ArrayBuilder};
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::types::{DataSize, DataType, DataTypeKind, DataTypeRef};

#[derive(Eq, PartialEq)]
pub struct StringType {
    nullable: bool,
    width: usize,
    kind: DataTypeKind,
}

impl std::fmt::Debug for StringType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StringType {{ nullable: {}, width: {}, kind: {:?} }}",
            self.nullable, self.width, self.kind
        )
    }
}

impl DataType for StringType {
    fn data_type_kind(&self) -> DataTypeKind {
        self.kind
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        Ok(Utf8ArrayBuilder::new(capacity)?.into())
    }

    fn to_protobuf(&self) -> Result<ProstDataType> {
        let mut prost = ProstDataType {
            precision: self.width as u32,
            is_nullable: self.nullable,
            ..Default::default()
        };

        match self.kind {
            DataTypeKind::Char => {
                prost.set_type_name(TypeName::Char);
                Ok(prost)
            }
            DataTypeKind::Varchar => {
                prost.set_type_name(TypeName::Varchar);
                Ok(prost)
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

impl<'a> TryFrom<&'a ProstDataType> for StringType {
    type Error = RwError;

    fn try_from(prost: &'a ProstDataType) -> Result<Self> {
        match prost.get_type_name() {
            TypeName::Char | TypeName::Symbol => Ok(Self {
                nullable: prost.is_nullable,
                width: prost.precision as usize,
                kind: DataTypeKind::Char,
            }),
            TypeName::Varchar => Ok(Self {
                nullable: prost.is_nullable,
                width: prost.precision as usize,
                kind: DataTypeKind::Varchar,
            }),
            _ => Err(InternalError(format!(
                "Incorrect data type kind for string type: {:?}",
                prost.get_type_name()
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
