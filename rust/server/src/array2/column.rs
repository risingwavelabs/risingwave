use crate::array2::ArrayRef;

use crate::error::ErrorCode::ProtobufError;
use crate::error::RwError;
use crate::types::DataTypeRef;
use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::Column as ColumnProto;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder, Clone, Debug)]
pub struct Column {
    pub(crate) array: ArrayRef,
    pub(crate) data_type: DataTypeRef,
}

impl Column {
    pub fn to_protobuf(&self) -> crate::error::Result<AnyProto> {
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
}
