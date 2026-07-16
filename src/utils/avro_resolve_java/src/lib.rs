use std::io::Read;

use apache_avro::types::Value;
use apache_avro::{AvroResult, Schema};

pub mod default;
pub mod resolver;

pub fn from_avro_datum<R: Read>(
    writer_schema: &Schema,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    let v = apache_avro::from_avro_datum(writer_schema, reader, None)?;
    let Some(reader_schema) = reader_schema else {
        return Ok(v);
    };
    let writer_schema = resolver::apply_aliases(writer_schema, reader_schema);
    let (root, seen) = resolver::resolve(&writer_schema, reader_schema);
    apply(&root, v, &seen)
}

fn apply(
    action: &resolver::Action<'_>,
    v: Value,
    seen: &resolver::RecordPairMap<'_>,
) -> AvroResult<Value> {
    use resolver::Action as A;

    let rv = match action {
        // TODO: DoNothing for Int->Date
        A::DoNothing { .. } => v,
        A::Skip { writer: _ } => unreachable!(),
        A::Container {
            writer: _,
            reader: _,
            element_action,
        } => match v {
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|item| apply(element_action, item, seen))
                    .collect::<AvroResult<_>>()?,
            ),
            Value::Map(items) => Value::Map(
                items
                    .into_iter()
                    .map(|(k, v)| apply(element_action, v, seen).map(|v| (k, v)))
                    .collect::<AvroResult<_>>()?,
            ),
            _ => unreachable!(),
        },
        A::Promote { writer: _, reader } => match (v, reader) {
            (Value::Int(i), Schema::Long) => Value::Long(i as i64),
            (Value::Int(i), Schema::Float) => Value::Float(i as f32),
            (Value::Long(l), Schema::Float) => Value::Float(l as f32),
            (Value::Int(i), Schema::Double) => Value::Double(i as f64),
            (Value::Long(l), Schema::Double) => Value::Double(l as f64),
            (Value::Float(f), Schema::Double) => Value::Double(f as f64),
            // promotion is UTF-8 here, unlike ISO-8859-1 for record field default
            (Value::String(s), Schema::Bytes) => Value::Bytes(s.into_bytes()),
            (Value::Bytes(b), Schema::String) => {
                Value::String(String::from_utf8_lossy(&b).into_owned())
            }
            // Logical types promotion are based on physical and LOST their meaning,
            // so they are NOT handled.
            // For example, TimeMillis(2) promotes to TimeMicros(2), changing 2ms to 2us
            _ => todo!("allowed statically but failed on runtime value"),
        },
        A::Error(inner) => {
            return Err(apache_avro::error::Details::DeserializeValue(inner.to_string()).into());
        }
        A::EnumAdjust(inner) => {
            let Value::Enum(writer_index, symbol) = v else {
                unreachable!()
            };
            inner
                .apply(writer_index, symbol)
                .map_err(apache_avro::error::Details::DeserializeValue)?
        }
        A::WriterUnion(inner) => {
            let Value::Union(writer_index, v) = v else {
                unreachable!()
            };
            let rv = apply(inner.get_action(writer_index as _), *v, seen)?;
            if inner.union_equiv() {
                Value::Union(writer_index, Box::new(rv))
            } else {
                rv
            }
        }
        A::ReaderUnion(inner) => {
            let (reader_index, action) = inner.get_match();
            let rv = apply(action, v, seen)?;
            Value::Union(reader_index as _, Box::new(rv))
        }
        A::RecordAdjust { pair } => {
            let inner = match &seen[pair] {
                Ok(inner) => inner,
                Err(action) => return apply(action, v, seen),
            };
            let Value::Record(field_values) = v else {
                unreachable!()
            };
            let reader_order = inner.reader_order();
            let mut reader_values = vec![(String::new(), Value::Null); reader_order.len()];
            let mut reader_order_idx = 0;
            for ((_old_name, v), a) in field_values.into_iter().zip(inner.actions()) {
                if matches!(a, A::Skip { .. }) {
                    continue;
                }
                let reader_field = &reader_order[reader_order_idx];
                // _old_name is deserialized before applying aliases
                // assert_eq!(reader_field.name, _old_name);
                let rv = apply(a, v, seen)?;
                reader_values[reader_field.position] = (reader_field.name.clone(), rv);
                reader_order_idx += 1;
            }
            assert_eq!(reader_order_idx, inner.first_default());
            for d in inner.defaults() {
                let reader_field = &reader_order[reader_order_idx];
                reader_values[reader_field.position] = (reader_field.name.clone(), d.clone());
                reader_order_idx += 1;
            }
            assert_eq!(reader_order_idx, reader_order.len());

            Value::Record(reader_values)
        }
    };
    Ok(rv)
}
