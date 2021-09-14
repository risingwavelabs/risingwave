use crate::array2::value_reader::{
    F32ValueReader, F64ValueReader, I16ValueReader, I32ValueReader, I64ValueReader, ValueReader,
};
use crate::array2::{
    ArrayBuilder, ArrayImpl, ArrayRef, PrimitiveArrayBuilder, PrimitiveArrayItemType,
};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::types::{build_from_proto, DataType, DataTypeRef};
use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::{Column as ColumnProto, DataType_TypeName};
use std::sync::Arc;

/// Column is owned by DataChunk. It consists of logic data type and physical array implementation.
#[derive(Clone, Debug)]
pub struct Column {
    array: ArrayRef,
    data_type: DataTypeRef,
}

fn read_numeric_column<T: PrimitiveArrayItemType, R: ValueReader<T>>(
    col: &ColumnProto,
    cardinality: usize,
) -> Result<ArrayRef> {
    let mut builder = PrimitiveArrayBuilder::<T>::new(cardinality)?;
    for buf in col.get_values() {
        let v = R::read(buf)?;
        builder.append(Some(v))?;
    }
    let arr = builder.finish()?;
    Ok(Arc::new(arr.into()))
}

impl Column {
    pub fn new(array: ArrayRef, data_type: DataTypeRef) -> Column {
        Column { array, data_type }
    }

    pub fn to_protobuf(&self) -> Result<AnyProto> {
        let mut column = ColumnProto::new();
        let proto_data_type = self.data_type.to_protobuf()?;
        column.set_column_type(proto_data_type);
        column.set_null_bitmap(self.array.null_bitmap().to_protobuf()?);
        let values = self.array.to_protobuf()?;
        // column.set_null_bitmap(bitmap);
        for (_idx, buf) in values.into_iter().enumerate() {
            column.mut_values().push(buf);
        }

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }

    pub fn from_protobuf(col: ColumnProto, cardinality: usize) -> Result<Self> {
        let array = match col.get_column_type().get_type_name() {
            DataType_TypeName::INT16 => {
                read_numeric_column::<i16, I16ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::INT32 => {
                read_numeric_column::<i32, I32ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::INT64 => {
                read_numeric_column::<i64, I64ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::FLOAT => {
                read_numeric_column::<f32, F32ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::DOUBLE => {
                read_numeric_column::<f64, F64ValueReader>(&col, cardinality)?
            }
            _ => {
                return Err(RwError::from(InternalError(
                    "unsupported conversion from Column to Array".to_string(),
                )))
            }
        };
        let data_type = build_from_proto(col.get_column_type())?;
        Ok(Self { array, data_type })
    }

    pub fn array(&self) -> ArrayRef {
        self.array.clone()
    }

    pub fn array_ref(&self) -> &ArrayImpl {
        &*self.array
    }

    pub fn data_type(&self) -> DataTypeRef {
        self.data_type.clone()
    }

    pub fn data_type_ref(&self) -> &dyn DataType {
        &*self.data_type
    }
}
