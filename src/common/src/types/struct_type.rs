use super::DataType;
use crate::array::ArrayMeta;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructType {
    pub fields: Vec<DataType>,
    pub field_names: Vec<String>,
}

impl StructType {
    pub fn new(named_fields: Vec<(DataType, String)>) -> Self {
        let mut fields = Vec::with_capacity(named_fields.len());
        let mut field_names = Vec::with_capacity(named_fields.len());
        for (d, s) in named_fields {
            fields.push(d);
            field_names.push(s);
        }
        Self {
            fields,
            field_names,
        }
    }

    pub fn to_array_meta(&self) -> ArrayMeta {
        ArrayMeta::Struct {
            children: self.fields.clone().into(),
        }
    }
}
