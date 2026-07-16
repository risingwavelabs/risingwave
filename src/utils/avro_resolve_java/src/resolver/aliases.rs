use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Schema;
use apache_avro::schema::Name;

pub fn apply_aliases<'s>(writer: &'s Schema, reader: &Schema) -> Cow<'s, Schema> {
    // todo: if writer == reader

    let mut aliases = HashMap::new();
    let mut field_aliases = HashMap::new();
    get_aliases(reader, &mut aliases, &mut field_aliases);
    if aliases.is_empty() && field_aliases.is_empty() {
        return Cow::Borrowed(writer);
    }
    apply_aliases_inner(writer, &aliases, &field_aliases)
}

#[macro_export]
macro_rules! types {
    (primitive) => {
        Schema::Null
            | Schema::Boolean
            | Schema::Int
            | Schema::Long
            | Schema::Float
            | Schema::Double
            | Schema::Bytes
            | Schema::String
    };
    (temporal) => {
        Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos
    };
}

fn get_aliases(
    reader: &Schema,
    aliases: &mut HashMap<Name, Name>,
    field_aliases: &mut HashMap<Name, Arc<HashMap<String, String>>>,
) {
    let named = match reader {
        Schema::Record(inner) => Some((&inner.name, &inner.aliases)),
        Schema::Enum(inner) => Some((&inner.name, &inner.aliases)),
        Schema::Fixed(inner) => Some((&inner.name, &inner.aliases)),
        // unnamed types
        types!(primitive) | Schema::Array(_) | Schema::Map(_) | Schema::Union(_) => None,
        // visit definitions but not references
        Schema::Ref { .. } => None,
        // logical types on unnamed types
        types!(temporal) | Schema::BigDecimal => None,
        // logical types that could wrap a fixed type
        Schema::Decimal(inner) => match inner.inner.as_ref() {
            Schema::Fixed(inner) => Some((&inner.name, &inner.aliases)),
            _ => None,
        },
        Schema::Uuid | Schema::Duration => None, /* todo: apache_avro does not expose underlying fixed schema */
    };
    if let Some((name, alias_list)) = named {
        for alias in alias_list.iter().flatten() {
            aliases.insert(alias.fully_qualified_name(&None), name.clone());
        }
    }
    match reader {
        // visit definitions but not references
        Schema::Ref { .. } => {}
        // leaves of the schema tree
        types!(primitive) | Schema::Enum(_) | Schema::Fixed(_) => {}
        // leaves of the schema tree as logical types
        types!(temporal)
        | Schema::BigDecimal
        | Schema::Decimal(_)
        | Schema::Uuid
        | Schema::Duration => {}
        // recursive types
        Schema::Union(inner) => {
            for s in inner.variants() {
                get_aliases(s, aliases, field_aliases);
            }
        }
        Schema::Array(inner) => {
            get_aliases(&inner.items, aliases, field_aliases);
        }
        Schema::Map(inner) => {
            get_aliases(&inner.types, aliases, field_aliases);
        }
        Schema::Record(inner) => {
            let mut record_aliases = HashMap::new();
            for field in &inner.fields {
                if let Some(aliases) = &field.aliases {
                    for field_alias in aliases {
                        record_aliases.insert(field_alias.clone(), field.name.clone());
                    }
                }
                get_aliases(&field.schema, aliases, field_aliases);
            }
            let record_aliases = Arc::new(record_aliases);
            if !record_aliases.is_empty() {
                field_aliases.insert(inner.name.clone(), record_aliases.clone());
                for record_alias in inner.aliases.iter().flatten() {
                    let n = record_alias.fully_qualified_name(&None);
                    field_aliases.insert(n, record_aliases.clone());
                }
            }
        }
    }
}

fn apply_aliases_inner<'s>(
    writer: &'s Schema,
    aliases: &HashMap<Name, Name>,
    field_aliases: &HashMap<Name, Arc<HashMap<String, String>>>,
) -> Cow<'s, Schema> {
    match writer {
        types!(primitive) => Cow::Borrowed(writer),
        types!(temporal) | Schema::BigDecimal => Cow::Borrowed(writer),
        Schema::Decimal(inner) => {
            let child = apply_aliases_inner(&inner.inner, aliases, field_aliases);
            match child {
                Cow::Borrowed(_) => Cow::Borrowed(writer),
                Cow::Owned(new) => {
                    let mut inner = inner.clone();
                    inner.inner = Box::new(new);
                    Cow::Owned(Schema::Decimal(inner))
                }
            }
        }
        Schema::Uuid | Schema::Duration => Cow::Borrowed(writer),
        Schema::Ref { name } => match aliases.get(name) {
            Some(new) => Cow::Owned(Schema::Ref { name: new.clone() }),
            None => Cow::Borrowed(writer),
        },
        Schema::Enum(inner) => match aliases.get(&inner.name) {
            Some(new) => {
                let mut inner = inner.clone();
                inner.name = new.clone();
                Cow::Owned(Schema::Enum(inner))
            }
            None => Cow::Borrowed(writer),
        },
        Schema::Fixed(inner) => match aliases.get(&inner.name) {
            Some(new) => {
                let mut inner = inner.clone();
                inner.name = new.clone();
                Cow::Owned(Schema::Fixed(inner))
            }
            None => Cow::Borrowed(writer),
        },
        Schema::Array(inner) => {
            let child = apply_aliases_inner(&inner.items, aliases, field_aliases);
            match child {
                Cow::Borrowed(_) => Cow::Borrowed(writer),
                Cow::Owned(new) => {
                    let mut inner = inner.clone();
                    inner.items = Box::new(new);
                    Cow::Owned(Schema::Array(inner))
                }
            }
        }
        Schema::Map(inner) => {
            let child = apply_aliases_inner(&inner.types, aliases, field_aliases);
            match child {
                Cow::Borrowed(_) => Cow::Borrowed(writer),
                Cow::Owned(new) => {
                    let mut inner = inner.clone();
                    inner.types = Box::new(new);
                    Cow::Owned(Schema::Map(inner))
                }
            }
        }
        Schema::Union(inner) => {
            let mut variants = Vec::new(); // do not allocate until necessary
            let mut changed = false;
            for (i, child) in inner.variants().iter().enumerate() {
                let new = apply_aliases_inner(child, aliases, field_aliases);
                if !changed {
                    if matches!(new, Cow::Borrowed(_)) {
                        continue;
                    }

                    changed = true;
                    variants.reserve(inner.variants().len());
                    variants.extend(inner.variants().iter().take(i).cloned());
                }
                variants.push(new.into_owned());
            }
            match changed {
                true => Cow::Owned(Schema::Union(
                    apache_avro::schema::UnionSchema::new(variants)
                        .expect("union remains valid after aliasing"),
                )),
                false => Cow::Borrowed(writer),
            }
        }
        Schema::Record(inner) => {
            let mut fields = Vec::new(); // do not allocate until necessary
            let new_name = match aliases.get(&inner.name) {
                Some(new) => Cow::Owned(new.clone()),
                None => Cow::Borrowed(&inner.name),
            };
            let mut fields_changed = false;
            let record_aliases = field_aliases.get(&new_name);
            for (i, field) in inner.fields.iter().enumerate() {
                let new_field_name =
                    match record_aliases.and_then(|aliases| aliases.get(&field.name)) {
                        Some(new) => Cow::Owned(new.clone()),
                        None => Cow::Borrowed(&field.name),
                    };
                let new_field_schema = apply_aliases_inner(&field.schema, aliases, field_aliases);
                if !fields_changed {
                    if matches!(new_field_name, Cow::Borrowed(_))
                        && matches!(new_field_schema, Cow::Borrowed(_))
                    {
                        continue;
                    }

                    fields_changed = true;
                    fields.reserve(inner.fields.len());
                    fields.extend(inner.fields.iter().take(i).cloned());
                }
                let mut new_field = field.clone();
                if let Cow::Owned(new) = new_field_name {
                    new_field.name = new;
                }
                if let Cow::Owned(new) = new_field_schema {
                    new_field.schema = new;
                }
                fields.push(new_field);
            }
            if matches!(new_name, Cow::Borrowed(_)) && !fields_changed {
                return Cow::Borrowed(writer);
            }
            let mut inner = inner.clone();
            if let Cow::Owned(new_name) = new_name {
                inner.name = new_name;
            }
            if fields_changed {
                inner.fields = fields;
                // nit: shall I use exhaustive construction
                inner.lookup = inner
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name.clone(), i))
                    .collect();
            }
            Cow::Owned(Schema::Record(inner))
        }
    }
}
