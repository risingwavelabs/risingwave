use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan_common::ColumnDesc;

pub trait ColumnDescTestExt {
    fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self;

    fn new_struct(name: &str, column_id: i32, type_name: &str, fields: Vec<ColumnDesc>) -> Self;
}

impl ColumnDescTestExt for ColumnDesc {
    fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            column_type: Some(data_type),
            column_id,
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn new_struct(name: &str, column_id: i32, type_name: &str, fields: Vec<ColumnDesc>) -> Self {
        Self {
            column_type: Some(DataType {
                type_name: TypeName::Struct as i32,
                is_nullable: true,
                ..Default::default()
            }),
            column_id,
            name: name.to_string(),
            type_name: type_name.to_string(),
            field_descs: fields,
        }
    }
}
