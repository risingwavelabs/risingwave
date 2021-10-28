use crate::types::DataTypeRef;

/// the field in the schema of the executor's return data
#[derive(Clone)]
pub struct Field {
    // TODO: field_name
    pub data_type: DataTypeRef,
}

/// the schema of the executor's return data
#[derive(Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}
