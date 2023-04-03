use arrow_schema::DataType;

pub fn data_types_match(a: &[&DataType], b: &[&DataType]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    (a.iter().zip(b.iter())).all(|(a, b)| data_type_match(a, b))
}

fn data_type_match(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        (List(a_inner), List(b_inner)) | (LargeList(a_inner), LargeList(b_inner)) => {
            data_type_match(a_inner.data_type(), b_inner.data_type())
        }
        (Struct(a_fields), Struct(b_fields)) => {
            if a_fields.len() != b_fields.len() {
                return false;
            }
            (a_fields.iter().zip(b_fields.iter()))
                .all(|(a, b)| data_type_match(&a.data_type(), &b.data_type()))
        }
        (a, b) => a == b,
    }
}
