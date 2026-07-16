use apache_avro::Schema;
use apache_avro::schema::NamesRef;

use super::Action;

pub fn resolve<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'_>,
    r: &'s Schema,
    r_names: &NamesRef<'_>,
) -> Action<'s> {
    if is_valid(w, w_names, r, r_names) {
        Action::Promote {
            writer: w,
            reader: r,
        }
    } else {
        super::error::ErrorType::IncompatibleSchemaTypes.act(w, r)
    }
}

fn is_valid(w: &Schema, w_names: &NamesRef<'_>, r: &Schema, r_names: &NamesRef<'_>) -> bool {
    use super::super::SchemaType as T;
    let w_type = T::from_schema(w, w_names);
    let r_type = T::from_schema(r, r_names);
    assert_ne!(
        w_type, r_type,
        "Only use when reader and writer are different."
    );

    matches!(
        (r_type, w_type),
        (T::Long, T::Int)
            | (T::Float, T::Int | T::Long)
            | (T::Double, T::Int | T::Long | T::Float)
            | (T::Bytes | T::String, T::String | T::Bytes)
    )
}
