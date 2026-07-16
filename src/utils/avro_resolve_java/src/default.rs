use apache_avro::Schema;
use apache_avro::schema::NamesRef;
use apache_avro::types::Value;
use serde_json::Value as J;

fn decode_bytes(s: &str) -> Option<Vec<u8>> {
    s.chars().map(|c| c.try_into().ok()).collect()
}

pub fn decode_default(s: &Schema, names: &NamesRef<'_>, j: &J) -> Option<Value> {
    use serde::Deserialize as D;

    let physical = match s {
        Schema::Ref { name } => names.get(name).and_then(|s| decode_default(s, names, j)),
        Schema::Null => j.is_null().then_some(Value::Null),
        Schema::Boolean => j.as_bool().map(Value::Boolean),
        Schema::Int | Schema::Date | Schema::TimeMillis => {
            Some(Value::Int(D::deserialize(j).ok()?))
        }
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => Some(Value::Long(D::deserialize(j).ok()?)),
        Schema::Float => Some(Value::Float(D::deserialize(j).ok()?)),
        Schema::Double => Some(Value::Double(D::deserialize(j).ok()?)),
        Schema::Bytes | Schema::Decimal(_) | Schema::BigDecimal => {
            j.as_str().and_then(decode_bytes).map(Value::Bytes)
        }
        Schema::String | Schema::Uuid => j.as_str().map(|s| Value::String(s.to_owned())),
        Schema::Fixed(inner) => j
            .as_str()
            .and_then(decode_bytes)
            .filter(|bytes| bytes.len() == inner.size)
            .map(|bytes| Value::Fixed(inner.size, bytes)),
        Schema::Duration => j
            .as_str()
            .and_then(decode_bytes)
            .filter(|bytes| bytes.len() == 12)
            .map(|bytes| Value::Fixed(bytes.len(), bytes)),
        Schema::Enum(inner) => j
            .as_str()
            .and_then(|s| inner.symbols.iter().enumerate().find(|(_, sym)| *sym == s))
            .map(|(idx, s)| Value::Enum(idx as _, s.to_owned())),
        Schema::Record(inner) => j.as_object().and_then(|obj| {
            let mut fields = Vec::with_capacity(inner.fields.len());
            for field in &inner.fields {
                let value = if let Some(value) = obj.get(&field.name) {
                    decode_default(&field.schema, names, value)
                } else if let Some(def) = &field.default {
                    decode_default(&field.schema, names, def)
                } else {
                    None
                };
                fields.push((field.name.clone(), value?));
            }
            Some(Value::Record(fields))
        }),
        Schema::Array(inner) => j.as_array().and_then(|arr| {
            let items = arr
                .iter()
                .map(|item| decode_default(&inner.items, names, item))
                .collect::<Option<_>>()?;
            Some(Value::Array(items))
        }),
        Schema::Map(inner) => j.as_object().and_then(|obj| {
            let items = obj
                .iter()
                .map(|(k, v)| decode_default(&inner.types, names, v).map(|v| (k.clone(), v)))
                .collect::<Option<_>>()?;
            Some(Value::Map(items))
        }),
        Schema::Union(inner) => {
            let first = inner.variants().first()?;
            let v = decode_default(first, names, j)?;
            Some(Value::Union(0, Box::new(v)))
        }
    };

    match s {
        crate::types!(primitive)
        | Schema::Array(_)
        | Schema::Map(_)
        | Schema::Union(_)
        | Schema::Record(_)
        | Schema::Enum(_)
        | Schema::Fixed(_) => physical,
        // Ref can be logical but already resolved inside the recursive call.
        Schema::Ref { .. } => physical,
        crate::types!(temporal)
        | Schema::Uuid
        | Schema::Duration
        | Schema::Decimal(_)
        | Schema::BigDecimal => physical?.resolve(s).ok(),
    }
}
